package sqlx

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
)

type reportErrors struct {
	Payload string      `json:"payload,omitempty"`
	Args    interface{} `json:"args,omitempty"`
	Error   error       `json:"error,omitempty"`
}

type webhookMessage struct {
	MsgType string `json:"msgtype,omitempty"`
	Text    struct {
		Content string `json:"content,omitempty"`
	} `json:"text,omitempty"`
}

func (e webhookMessage) String() string {
	b, _ := jsoniter.Marshal(e)
	return string(b)
}

func (e reportErrors) String() string {
	return fmt.Sprintf("数据库解析增量失败:\n 【SQL：】%s \n\n 【Args:】%v \n\n【ERROR：】%v", e.Payload, e.Args, e.Error)
}
