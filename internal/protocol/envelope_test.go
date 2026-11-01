package protocol

import (
	"errors"
	"testing"
)

// 编码结果必须是一行：以 '\n' 结尾，且空字段不出现在 JSON 里。
func TestEncode(t *testing.T) {
	b, err := Encode(Envelope{Type: TypeWho})
	if err != nil {
		t.Fatalf("Encode 返回错误: %v", err)
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		t.Fatalf("必须以换行结尾，实际 %q", string(b))
	}
	want := `{"type":"who"}` + "\n"
	if string(b) != want {
		t.Fatalf("got %q, want %q", string(b), want)
	}
}

// 编码再解码，字段一个都不能丢。
func TestRoundTrip(t *testing.T) {
	in := Envelope{
		Type:     TypeChat,
		MsgID:    "m-1",
		From:     "alice",
		To:       "bob",
		ChatType: "single",
		Body:     "你好",
	}
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode 返回错误: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode 返回错误: %v", err)
	}
	if out != in {
		t.Fatalf("往返后不一致:\n got %+v\nwant %+v", out, in)
	}
}

// 分帧边界：空行、CRLF、坏 JSON、缺 type。
func TestDecode(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		wantErr error
		isErr   bool
	}{
		{name: "正常行", line: `{"type":"chat","body":"你好"}` + "\n"},
		{name: "容忍CRLF", line: `{"type":"chat","body":"你好"}` + "\r\n"},
		{name: "空行", line: "", wantErr: ErrEmptyLine},
		{name: "只有换行", line: "\n", wantErr: ErrEmptyLine},
		{name: "半条消息", line: `{"type":`, isErr: true},
		{name: "缺type", line: `{"body":"你好"}` + "\n", isErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode([]byte(c.line))
			switch {
			case c.wantErr != nil:
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("got %v, want %v", err, c.wantErr)
				}
			case c.isErr:
				if err == nil {
					t.Fatalf("应该返回错误，实际 nil")
				}
			default:
				if err != nil {
					t.Fatalf("不应该报错: %v", err)
				}
			}
		})
	}
}
