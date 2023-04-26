package sqlx

import (
	"fmt"
	"strings"
	"testing"
)

type parseStmt struct {
	SQL          string
	Args         []interface{}
	ExceptNewSQL string
	ExceptArgs   []string
	PkExists     bool
}

var stmts = []parseStmt{
	//{
	//	SQL:          "update fx_task as a,fx_user u set a.title= ?,a.parent_id=? where a.id=? and a.creator_id = u.id",
	//	Args:         []interface{}{"xxxxfdsf", 42342342, 3456},
	//	ExceptNewSQL: "INSERT INTO incre_tasks (task_id,key,value) VALUES(?,?,?)",
	//	ExceptArgs:   []string{"3456", "title", "xxxxfdsf", "3456", "parent_id", "42342342"},
	//	PkExists:     true,
	//},
	//{
	//	SQL:          "update fx_task a set a.title= ?,a.matter_type=? where a.id=? and a.x=800",
	//	Args:         []interface{}{"xxxxfdsf", "1111111111", 3456},
	//	ExceptNewSQL: "INSERT INTO incre_tasks (task_id,key,value) VALUES(?,?,?)",
	//	ExceptArgs:   []string{"3456", "title", "xxxxfdsf"},
	//	PkExists:     true,
	//},
	//{
	//	SQL:          "update fx_user  set nick_name= ?,avatar=?,city=? where id=? ",
	//	Args:         []interface{}{"nick", "https://xx/x.jpg", "guangzhou", 3456},
	//	ExceptNewSQL: "INSERT INTO incre_users(creator_id,key,value) VALUES(?,?,?)",
	//	ExceptArgs:   []string{"3456", "nick_name", "nick", "3456", "avatar", "https://xx/x.jpg"},
	//	PkExists:     true,
	//},
	//
	//{
	//	SQL:          "update fx_task_dispatch  set invite_id = ?, personal_state=?,reason=? where dispatch_id=? ",
	//	Args:         []interface{}{"23453", "10801", "什么情况", 542319097939206},
	//	ExceptNewSQL: "INSERT INTO incre_dispatchs(dispatch_id,ref_task_id,key,value) VALUES(?,?,?,?)",
	//	ExceptArgs:   []string{"542319097939206", "", "personal_state", "10801", "542319097939206", "", "reason", "什么情况"},
	//	PkExists:     false,
	//},
	//{
	//	SQL:          "update fx_task_dispatch  set invite_id = ?, personal_state=?,reason=? where dispatch_id=? ",
	//	Args:         []interface{}{"321312390890", "10453", "什么情况4324234", 542319097939206},
	//	ExceptNewSQL: "INSERT INTO incre_dispatchs(dispatch_id,ref_task_id,key,value) VALUES(?,?,?,?)",
	//	ExceptArgs:   []string{"542319097939206", "", "personal_state", "10453", "542319097939206", "", "reason", "什么情况4324234"},
	//	PkExists:     false,
	//},
	//{
	//	SQL:        "UPDATE fx_task_dispatch SET status = 0, is_valid = 0, update_at = ? WHERE dispatch_id IN (?, ?) AND is_valid = 1",
	//	Args:       []interface{}{42314123412341234, 45768798, 93425678},
	//	ExceptArgs: []string{"45768798", "", "update_at", "42314123412341234", "93425678", "", "update_at", "42314123412341234"},
	//},
	//{
	//	SQL:  "UPDATE fx_task_dispatch SET personal_state=?,widget=?,reason=?,accept_at=? WHERE dispatch_id IN (?) AND status=?",
	//	Args: []interface{}{"state-10801", &map[string]interface{}{"x": "widget"}, "reason-3456", "accept-at23432432", "id-1,id-2,id-3", 45},
	//	ExceptArgs: []string{"id-1", "", "personal_state", "state-10801", "id-2", "", "personal_state", "state-10801", "id-3", "",
	//		"personal_state", "state-10801", "id-1", "", "reason", "reason-3456", "id-2", "", "reason", "reason-3456", "id-3", "",
	//		"reason", "reason-3456", "id-1", "", "accept_at", "accept-at23432432", "id-2", "", "accept_at", "accept-at23432432", "id-3", "", "accept_at", "accept-at23432432"},
	//},
	//{
	//	SQL:        "UPDATE fx_task_dispatch SET state = ?, finish_time = ?, update_at = ? WHERE dispatch_id != ? AND ref_task_id = ? AND state = ? AND is_valid = 1",
	//	Args:       []interface{}{"state-01", "ft-3456", "ut-4567890", "disp-543534534534534", "task-534678", "cond-state-34567"},
	//	ExceptArgs: []string{"", "task-534678", "state", "state-01", "", "task-534678", "finish_time", "ft-3456", "", "task-534678", "update_at", "ut-4567890"},
	//},
	//{
	//	SQL:        "update fx_user_setting set svalue=? ,update_at=? where user_id=? and setting_key=?",
	//	Args:       []interface{}{"xxx", 23456789, 4567456754324567, "file_policy"},
	//	ExceptArgs: []string{"4567456754324567", "svalue", "xxx", "4567456754324567", "setting_key", "file_policy"},
	//},
	//{
	//	SQL:          "update fx_task_dispatch  set personal_state=? where ref_task_id=?  and is_valid=1",
	//	Args:         []interface{}{"10453", 542321582801158},
	//	ExceptNewSQL: "INSERT INTO incre_dispatchs(dispatch_id,ref_task_id,key,value) VALUES(?,?,?,?)",
	//	ExceptArgs:   []string{"", "542321582801158", "personal_state", "10453"},
	//	PkExists:     true,
	//},
	//{
	//	SQL:        "UPDATE fx_task_dispatch SET status = 0, is_valid = 0, update_at = ? WHERE dispatch_id IN (?) AND is_valid = 1",
	//	Args:       []interface{}{1622618326, "889239196669056"},
	//	ExceptArgs: []string{"889239196669056", "", "update_at", "1622618326"},
	//},
	//{
	//	SQL:        "UPDATE fx_attachment SET file_name=? WHERE id=? AND creator_id=?",
	//	Args:       []interface{}{"aaa.jpg", 3535345, 89999999999},
	//	ExceptArgs: []string{"89999999999", "3535345", "file_name", "aaa.jpg"},
	//},
	//{
	//	SQL:        "UPDATE fx_attachment_share SET file_name=? WHERE file_id=? AND user_id=? AND cancel_share_at = 0",
	//	Args:       []interface{}{"fn.jpg", 434234234, 534545555555555},
	//	ExceptArgs: []string{"434234234", "534545555555555", "file_name", "fn.jpg"},
	//},
	{
		SQL: "UPDATE fx_task SET start_time = ?, end_time = ?, remind_at = ?, widget = ?, update_at = ? WHERE id = ? AND creator_id = ?",
		Args: []interface{}{1623823200, 1623826740, "{\"end_remind\":1623825840,\"start_remind\":1623823200}",
			"{\"execute_addr\":false,\"remind\":true,\"time\":true}", 1623820732, "875738326630677", "767158678716631"},
	},
	{
		SQL:        "UPDATE fx_task SET category = 0, parent_id = '', sort = 0, update_at = ? WHERE parent_id = ? AND category = 2",
		Args:       []interface{}{1622703991, "891104773537929,891138749235345"},
		ExceptArgs: []string{"", "category", "0", "", "parent_id", "", "", "update_at", "1622703991", "", "sort", "0"},
	},
	{
		SQL:        "update fx_attachment set creator_deleted_at=? where id=? and creator_id=?",
		Args:       []interface{}{1622637990, "889809877860552", "809473508114577"},
		ExceptArgs: []string{"809473508114577", "889809877860552", "creator_deleted_at", "1622637990"},
	},
	{
		SQL:        "update fx_task set remind_at=? where id=?",
		Args:       []interface{}{map[string]interface{}{"end_remind": 1623221100, "start_remind": 1623202200}, 904555358322690},
		ExceptArgs: []string{"904555358322690", "remind_at", `{"end_remind":1623221100,"start_remind":1623202200}`},
	},
	{
		SQL:        "UPDATE fx_task_config SET category = 0, parent_id = '', sort = 0, update_at = ? WHERE parent_id = ? AND category = 2",
		Args:       []interface{}{1622703991, "891104773537929,891138749235345"},
		ExceptArgs: []string{"", "category", "0", "", "parent_id", "", "", "update_at", "1622703991", "", "sort", "0"},
	},
}

func TestConfigParse(t *testing.T) {
	//sql, args, _ := In("UPDATE fx_task_dispatch SET status = 0, is_valid = 0, update_at = ? WHERE dispatch_id IN (?) AND is_valid = 1", 32456, []int{2323, 32323})
	//fmt.Println(sql, args)

	for _, stmt := range stmts {
		results, _ := ConfigArray.Parse(stmt.SQL, stmt.Args...)
		for _, sr := range results.Statements {
			var (
				v1        = strings.Join(sr.Args, ",")
				v2        = strings.Join(stmt.ExceptArgs, ",")
				argsCount = strings.Count(sr.NewItem, "?")
			)

			if v1 != v2 || argsCount != len(sr.Args) {
				t.Logf("NOT MATCH : \nSQL:%s \nActual(%d):%v \nExceptArgs(%d): %v \nInsertIncr:%s", stmt.SQL,
					len(sr.Args), v1,
					len(stmt.ExceptArgs), v2, sr.NewItem)

				t.Fail()
			}
		}
		fmt.Printf("`%s`,", results.String())
	}
	fmt.Println()
	//ConfigArray.Parse("update fx_task a set title= ? where id=?", "xxxxfdsf", 3456)

}

func BenchmarkSQLParse(b *testing.B) {
	b.ResetTimer()
	for _, stmt := range stmts {
		_, _ = ConfigArray.Parse(stmt.SQL, stmt.Args...)
	}
	b.StopTimer()
}
