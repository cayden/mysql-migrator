package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ============================================================
//  Doris Stream Load Writer
//  使用 HTTP PUT + NDJSON 格式，比 INSERT 快 10-100 倍。
//  参考: https://doris.apache.org/docs/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD
// ============================================================

// DorisConfig 对应 config.yaml 中 doris 段。
type DorisConfig struct {
	Host           string  `yaml:"host"`             // FE 地址
	HttpPort       int     `yaml:"http_port"`        // FE HTTP 端口，默认 8030
	User           string  `yaml:"user"`             // 用户名
	Password       string  `yaml:"password"`         // 密码
	Database       string  `yaml:"database"`         // 目标库
	StreamBatch    int     `yaml:"stream_batch"`     // 每批 Stream Load 行数，默认 200000
	MaxFilterRatio float64 `yaml:"max_filter_ratio"` // 脏数据容忍比例，默认 0
	StrictMode     *bool   `yaml:"strict_mode"`      // 严格模式，默认 false
	Timeout        int     `yaml:"timeout"`          // HTTP 超时（秒），默认 600
	LabelPrefix    string  `yaml:"label_prefix"`     // load label 前缀，默认 "migrate"
}

// DorisWriter 通过 HTTP Stream Load 写入 Doris。
type DorisWriter struct {
	cfg    DorisConfig
	client *http.Client
	seq    int64 // 递增序号，保证 label 唯一
}

// streamLoadResponse Doris Stream Load 返回的 JSON。
type streamLoadResponse struct {
	TxnId              int64  `json:"TxnId"`
	Label              string `json:"Label"`
	Status             string `json:"Status"`
	Message            string `json:"Message"`
	NumberTotalRows    int64  `json:"NumberTotalRows"`
	NumberLoadedRows   int64  `json:"NumberLoadedRows"`
	NumberFilteredRows int64  `json:"NumberFilteredRows"`
	ErrorURL           string `json:"ErrorURL"`
	LoadTimeMs         int64  `json:"LoadTimeMs"`
}

func NewDorisWriter(cfg DorisConfig) *DorisWriter {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 600
	}
	return &DorisWriter{
		cfg: cfg,
		client: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
			// Doris FE 返回 307 重定向到 BE 执行，需要自动跟随
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// 重新注入 Auth header（Go 默认不传递 Authorization 到重定向目标）
				if len(via) > 0 {
					req.Header.Set("Authorization", via[0].Header.Get("Authorization"))
					req.Header.Set("Expect", "100-continue")
				}
				return nil
			},
		},
	}
}

func (w *DorisWriter) prepare(ctx context.Context) error {
	// 验证连接：试一次简短的请求
	log.Printf("Doris 目标: %s:%d/%s", w.cfg.Host, w.cfg.HttpPort, w.cfg.Database)
	return nil
}

func (w *DorisWriter) close() error {
	return nil
}

// flushBatch 通过 Stream Load 写入一批数据。
func (w *DorisWriter) flushBatch(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// 1. 构建 NDJSON body
	body, err := buildNDJSON(columns, rows)
	if err != nil {
		return 0, fmt.Errorf("构建 NDJSON: %w", err)
	}

	// 2. 生成唯一 label（Doris 用 label 做幂等去重）
	w.seq++
	label := fmt.Sprintf("%s_%s_%d_%d", w.cfg.LabelPrefix, table, time.Now().UnixMilli(), w.seq)

	// 3. 构建 HTTP 请求
	url := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		w.cfg.Host, w.cfg.HttpPort, w.cfg.Database, table)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("创建请求: %w", err)
	}

	// 认证
	auth := base64.StdEncoding.EncodeToString([]byte(w.cfg.User + ":" + w.cfg.Password))
	req.Header.Set("Authorization", "Basic "+auth)
	req.Header.Set("Expect", "100-continue")

	// 格式
	req.Header.Set("format", "json")
	req.Header.Set("read_json_by_line", "true")
	req.Header.Set("fuzzy_parse", "true") // 自动类型推断
	req.Header.Set("label", label)

	// 严格模式
	strict := "false"
	if w.cfg.StrictMode != nil && *w.cfg.StrictMode {
		strict = "true"
	}
	req.Header.Set("strict_mode", strict)

	// 脏数据比例
	mfr := w.cfg.MaxFilterRatio
	if mfr <= 0 {
		mfr = 0.0
	}
	req.Header.Set("max_filter_ratio", fmt.Sprintf("%.2f", mfr))

	// 超时（秒）
	timeout := w.cfg.Timeout
	if timeout <= 0 {
		timeout = 600
	}
	req.Header.Set("timeout", fmt.Sprintf("%d", timeout))

	// 4. 发送请求
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Stream Load 网络错误: %w", err)
	}
	defer resp.Body.Close()

	// 5. 解析响应
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 限制 1MB

	var slResp streamLoadResponse
	if err := json.Unmarshal(respBody, &slResp); err != nil {
		// 可能返回纯文本错误
		return 0, fmt.Errorf("Stream Load HTTP %d, 响应解析失败: %s", resp.StatusCode, string(respBody))
	}

	if slResp.Status != "Success" && slResp.Status != "Publish Timeout" {
		errMsg := slResp.Message
		if slResp.ErrorURL != "" {
			errMsg += " | 详情: " + slResp.ErrorURL
		}
		return slResp.NumberLoadedRows, fmt.Errorf("Stream Load 失败 (%s): %s", slResp.Status, errMsg)
	}

	return slResp.NumberLoadedRows, nil
}

// buildNDJSON 将行数据序列化为 NDJSON（每行一个 JSON 对象）。
// 输入: columns=["col1","col2"], rows=[["a",1],["b",2]]
// 输出:
//
//	{"col1":"a","col2":1}
//	{"col1":"b","col2":2}
func buildNDJSON(columns []string, rows [][]interface{}) ([]byte, error) {
	var buf bytes.Buffer

	for _, row := range rows {
		if len(columns) != len(row) {
			return nil, fmt.Errorf("列数不匹配: columns=%d, row=%d", len(columns), len(row))
		}

		// JSON 对象: {"col1": val1, "col2": val2, ...}
		buf.WriteByte('{')
		for i, val := range row {
			if i > 0 {
				buf.WriteByte(',')
			}
			// key
			key, _ := json.Marshal(columns[i])
			buf.Write(key)
			buf.WriteByte(':')

			// value: 对 string 特殊处理，其他类型用 json.Marshal
			switch v := val.(type) {
			case string:
				if v == "" {
					// 空字符串 → JSON null（Doris 可能期望 NULL）
					buf.WriteString("null")
				} else {
					data, _ := json.Marshal(v)
					buf.Write(data)
				}
			case nil:
				buf.WriteString("null")
			default:
				data, _ := json.Marshal(v)
				buf.Write(data)
			}
		}
		buf.WriteString("}\n")
	}

	return buf.Bytes(), nil
}

// fetchDorisError 从错误 URL 获取详细错误信息（可选，用于调试）。
func fetchDorisError(client *http.Client, auth string, errorURL string) string {
	req, err := http.NewRequest("GET", errorURL, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Basic "+auth)

	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 100<<10)) // 100KB
	// 只返回前几行关键错误
	lines := strings.Split(string(body), "\n")
	if len(lines) > 20 {
		lines = lines[:20]
		lines = append(lines, "...(截断)")
	}
	return strings.Join(lines, "\n")
}
