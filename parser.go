package sqlx

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/xwb1989/sqlparser"
)

const IncrementalQueue = "flyele-incre-queue"

var (
	IncreNotSupportedError          = errors.New("不支持的SQL语句类型")
	foundNewIncrementalDataCallback func(payload string, args ...interface{})
	ConfigArray                     TableConfigs
	tableConfig                     = `[
  {
    "source_table_name": "fx_task",
	"source_table_pk":"id",
	"incre_stmt":"INSERT INTO incre_tasks (task_id,field,value) VALUES(?,?,?)",
	"incre_mapping":{
	   "task_id":"id"
	},
    "incre_fields": [
      "title",
      "detail",
      "files",
      "start_time",
      "start_time_full_day",
      "end_time",
      "end_time_full_day",
      "remind_at",
      "execute_addr",
      "widget",
      "state",
      "creator_id",
      "cancel_at",
      "update_at",
      "repeat_type",
      "end_repeat_at"
    ]
  },
  {
    "source_table_name": "fx_task_config",
	"source_table_pk":"id",
	"incre_stmt":"INSERT INTO incre_tasks (task_id,field,value) VALUES(?,?,?)",
	"incre_mapping":{
	   "task_id":"id"
	},
    "incre_fields": [
      "max_taker_total",
      "category",
      "parent_id",
      "sort",
      "ref_meeting_id",
      "update_at"
    ]
  },
  {
    "source_table_name": "fx_task_dispatch",
	"source_table_pk": "ref_task_id",
	"source_table_pk_index":1,
    "incre_stmt": "INSERT INTO incre_dispatchs(dispatch_id,ref_task_id,field,value) VALUES(?,?,?,?)",
	"incre_mapping":{
		"dispatch_id":"dispatch_id",
		"ref_task_id":"ref_task_id"
	},
    "incre_fields": [
      "creator_id",
      "identity",
      "state",
      "operate_state",
      "personal_state",
      "reason",
      "execute_at",
      "personal_remind_at",
      "finish_time",
      "accept_at",
      "cancel_at",
      "revoke_at",
      "delete_at",
      "update_at",
      "is_valid"
    ]
  },
  {
    "source_table_name": "fx_user",
    "incre_stmt": "INSERT INTO incre_users(creator_id,field,value) VALUES(?,?,?)",
    "source_table_pk": "id",
	"incre_mapping":{
		"creator_id":"id"
	},
    "incre_fields": [
      "avatar",
      "nick_name"
    ]
  },
  {
    "source_table_name": "fx_user_setting",
    "incre_stmt": "INSERT INTO incre_settings(creator_id,field,value) VALUES(?,?,?)",
    "source_table_pk": "user_id",
	"incre_mapping":{
		"creator_id":"user_id"
	},
    "incre_fields": [
      "setting_key",
      "svalue"
    ]
  },
  {
    "source_table_name": "fx_attachment",
    "incre_stmt": "INSERT INTO incre_attachment(creator_id,file_id,field,value) VALUES(?,?,?,?)",
    "source_table_pk": "id",
	"incre_mapping":{
		"creator_id":"creator_id",
		"file_id":"id"
	},
    "incre_fields": [
      "creator_deleted_at",
      "file_name"
    ]
  },
  {
    "source_table_name": "fx_attachment_share",
    "incre_stmt": "INSERT INTO incre_attachment(file_id,creator_id,field,value) VALUES(?,?,?,?)",
    "source_table_pk": "file_id",
	"incre_mapping":{
		"file_id":"file_id",
		"creator_id":"user_id"
	},
    "incre_fields": [
      "file_name",
	  "cancel_share_at"
    ]
  }
]`
	configMapping = make(map[string]*config)
)

type SqlIncrementalStatement struct {
	Statements   []*Statement `json:"statements,omitempty"`
	OriginalSQL  string       `json:"original_sql,omitempty"`  //原始SQL
	OriginalArgs interface{}  `json:"original_args,omitempty"` // 原始参数列表
}

type TableConfigs []*config

type config struct {
	SourceTableName             string            `json:"source_table_name"`         //需要监听的原始表名称，当update该表的 指定字段（incre_fileds）时，触发增量更新逻辑
	SourceTableDataPk           string            `json:"source_table_pk,omitempty"` //增量数据的主键
	IncreUpdateConditionMapping map[string]string `json:"incre_mapping,omitempty"`   //增量更新表中所需字段如何获取的映射逻辑
	SortedMappingKeys           []string          //对IncreUpdateConditionMapping的value进行排序后从原SQL参数中取值
	IncreStatement              string            `json:"incre_stmt,omitempty"` //更新的插入语句
	IncreFields                 []string          `json:"incre_fields"`         //原始表中需要监听的字段列表
}

type Statement struct {
	EffectedRows string   `json:"effected_rows,omitempty"`
	NewItem      string   `json:"new_item,omitempty"`
	Args         []string `json:"args,omitempty"`
}

type sqlParseResult struct {
	originalSQL       string
	originalArgs      interface{}
	kvMapping         map[string]map[string]string // SET 字段及值
	conditonMapping   map[string]map[string]string //条件 字段及值
	tableShortMapping map[string]string            //表别名映射关系
	whereClause       string                       // where语句
	updatedTables     string
}

func init() {
	_ = json.Unmarshal([]byte(tableConfig), &ConfigArray)
	for _, c := range ConfigArray {
		configMapping[c.SourceTableName] = c
		for _, tfield := range c.IncreUpdateConditionMapping {
			c.SortedMappingKeys = append(c.SortedMappingKeys, tfield)
		}
		sort.Strings(c.SortedMappingKeys)
	}
}

func SetIncrementalFoundedCallback(callback func(payload string, args ...interface{})) {
	foundNewIncrementalDataCallback = callback
}

func (c TableConfigs) Parse(sql string, args ...interface{}) (*SqlIncrementalStatement, error) {
	if strings.HasPrefix(sql, "update") || strings.HasPrefix(sql, "UPDATE") {
		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			return nil, err
		}
		switch stmt := stmt.(type) {
		case *sqlparser.Update:
			for i := 0; i < len(args); i++ {
				var (
					arg       = args[i]
					valueType = reflect.TypeOf(arg).Kind()
				)
				if valueType == reflect.Ptr || valueType == reflect.Map ||
					valueType == reflect.Struct || valueType == reflect.Slice ||
					valueType == reflect.Array {
					b, _ := json.Marshal(arg)
					args[i] = string(b)
				}
			}
			stmts := c.parseSQLStmt(stmt, sql, args...).updateStatementArgs()
			return stmts, nil
		}
		return nil, nil
	}
	return nil, IncreNotSupportedError
}

func (c TableConfigs) parseSQLStmt(stmt *sqlparser.Update, sql string, args ...interface{}) *sqlParseResult {
	var (
		resp = &sqlParseResult{
			kvMapping:         make(map[string]map[string]string),
			conditonMapping:   make(map[string]map[string]string),
			tableShortMapping: make(map[string]string),
			originalArgs:      args,
			originalSQL:       sql,
		}
		printBuf       = sqlparser.NewTrackedBuffer(nil)
		firstTableName string
		shouldParse    bool
	)

	for _, expr := range stmt.TableExprs {
		var t interface{} = expr

		printBuf.Reset()
		if aliased, ok := t.(*sqlparser.AliasedTableExpr); ok {
			aliased.Expr.Format(printBuf)
			var tableName = printBuf.String()
			if !aliased.As.IsEmpty() {
				resp.tableShortMapping[aliased.As.String()] = tableName
			}
			if _, ok := configMapping[tableName]; ok {
				shouldParse = true
			}
			firstTableName = tableName
		}
	}
	if !shouldParse {
		return resp
	}
	printBuf.Reset()
	stmt.TableExprs.Format(printBuf)
	resp.updatedTables = printBuf.String()
	var (
		updateFieldCount int = 0
	)
	for _, expr := range stmt.Exprs {
		var (
			fieldMetada  = expr.Name
			fieldName    = fieldMetada.Name.String()
			v            string
			valinterface interface{} = expr.Expr
		)
		if sqlVal, ok := valinterface.(*sqlparser.SQLVal); ok {
			var vs = string(sqlVal.Val)
			if strings.HasPrefix(vs, ":") {
				if len(args) > updateFieldCount {
					v = fmt.Sprintf("%v", args[updateFieldCount])
				}
				if fieldMetada.Qualifier.Name.IsEmpty() {
					//resp.addEffectedFields(firstTableName, fieldName)
					resp.updateKvMapping(resp.kvMapping, firstTableName, fieldName, v)
				} else {
					var tableName = resp.tableShortMapping[fieldMetada.Qualifier.Name.String()]
					resp.updateKvMapping(resp.kvMapping, tableName, fieldName, v)
				}
				updateFieldCount = updateFieldCount + 1
			} else {
				resp.updateKvMapping(resp.kvMapping, firstTableName, fieldName, vs)
			}
		}

	}
	var w = whereClauseParser{
		FirstTableName:   firstTableName,
		updateFieldCount: &updateFieldCount,
		Args:             args,
	}
	w.parse(stmt.Where.Expr, resp)
	if ex, ok := stmt.Where.Expr.(*sqlparser.AndExpr); ok {
		w.parse(ex.Left, resp)
		w.parse(ex.Right, resp)
	}
	printBuf.Reset()
	stmt.Where.Format(printBuf)
	resp.whereClause = printBuf.String()
	return resp
}

type whereClauseParser struct {
	FirstTableName   string
	updateFieldCount *int
	Args             []interface{}
}

func (w *whereClauseParser) parse(condExpr sqlparser.Expr, resp *sqlParseResult) {
	if condExpr == nil {
		return
	}
	if ex, ok := condExpr.(*sqlparser.RangeCond); ok {
		var from interface{} = ex.From
		var to interface{} = ex.To
		w.cacheConditions(from, ex.Left, resp, ex.Operator)
		w.cacheConditions(to, condExpr, resp, ex.Operator)
	} else if ex, ok := condExpr.(*sqlparser.AndExpr); ok {
		w.parse(ex.Left, resp)
		w.parse(ex.Right, resp)
	} else if ex, ok := condExpr.(*sqlparser.ComparisonExpr); ok {
		var right interface{} = ex.Right
		var left interface{} = ex.Left
		if rv, ok := right.(sqlparser.ValTuple); ok {
			for _, expr := range rv {
				if w.cacheConditions(expr, left, resp, ex.Operator) {
					return
				}
			}
		}
		if w.cacheConditions(right, left, resp, ex.Operator) {
			return
		}
	}

}

func (w *whereClauseParser) cacheConditions(right interface{}, left interface{}, resp *sqlParseResult, operator string) bool {
	if rv, ok := right.(*sqlparser.SQLVal); ok && strings.HasPrefix(string(rv.Val), ":") {
		if len(w.Args) <= *w.updateFieldCount {
			return true
		}
		var vp = fmt.Sprintf("'%v'", w.Args[*w.updateFieldCount])
		var v = fmt.Sprintf("%v", w.Args[*w.updateFieldCount])
		rv.Val = []byte(vp)

		if leftCol, ok := left.(*sqlparser.ColName); ok {
			if operator == "!=" {
				resp.updateKvMapping(resp.conditonMapping, w.FirstTableName, leftCol.Name.String(), "")
			} else if leftCol.Qualifier.Name.IsEmpty() {
				resp.updateKvMapping(resp.conditonMapping, w.FirstTableName, leftCol.Name.String(), v)
			} else {
				var tableName = resp.tableShortMapping[leftCol.Qualifier.Name.String()]
				resp.updateKvMapping(resp.conditonMapping, tableName, leftCol.Name.String(), v)
			}

		}
		*w.updateFieldCount = *w.updateFieldCount + 1
	}
	return false
}

func (s *sqlParseResult) updateKvMapping(store map[string]map[string]string, tableName, fieldName string, value string) {
	v, ok := store[tableName]
	if !ok {
		v = make(map[string]string)
	}
	if vs, ok := v[fieldName]; ok {
		v[fieldName] = fmt.Sprintf("%s,%s", vs, value)
	} else {
		v[fieldName] = value
	}
	store[tableName] = v
}

func (s *sqlParseResult) updateStatementArgs() *SqlIncrementalStatement {
	var response = &SqlIncrementalStatement{
		OriginalSQL:  s.originalSQL,
		OriginalArgs: s.originalArgs,
	}
	for table, v := range s.conditonMapping {
		for vf, vv := range v {
			if kv, ok := s.kvMapping[table]; ok {
				if _, ok := kv[vf]; !ok {
					kv[vf] = vv
				}
			}
		}
	}
	for srcTbName, kvMapping := range s.kvMapping {
		if cfg, ok := configMapping[srcTbName]; ok {
			var (
				increStmt = &Statement{
					Args:    make([]string, 0),
					NewItem: cfg.IncreStatement,
				}
				argMaxLen = strings.Count(cfg.IncreStatement, "?")
			)

			for i := 0; i < len(cfg.SortedMappingKeys); i++ {
				var srcField = cfg.SortedMappingKeys[i]
				var fieldValue = kvMapping[srcField]
				if fieldValue == "" {
					increStmt.EffectedRows = fmt.Sprintf("SELECT %s FROM %s %s", srcField, s.updatedTables, s.whereClause)
				}
				increStmt.Args = append(increStmt.Args, fieldValue)
			}

			var (
				tempArgs            = increStmt.Args[0:]
				hasIncrementalField bool
			)

			for k, v := range kvMapping {
				if isInArray(k, cfg.IncreFields) {
					hasIncrementalField = true
					if len(increStmt.Args) >= argMaxLen {
						increStmt.Args = append(increStmt.Args, tempArgs...)
					}
					if k != cfg.SourceTableDataPk && shouldSpitParameters(v) {
						v = strings.Split(v, ",")[0]
					}
					// 该字段需要做增量更新
					increStmt.Args = append(increStmt.Args, k, v)
				}
			}
			if hasIncrementalField {
				increStmt.fixConditions(argMaxLen)
				response.Statements = append(response.Statements, increStmt)
			}
		}
	}
	return response
}

func (s *Statement) fixUpdateStmtPlaceholder(argMaxLen int) {
	if (len(s.Args)/argMaxLen - 1) == 0 {
		return
	}
	var (
		argsPlaceholder []string
		argsHolderArray []string
	)
	for i := 0; i < argMaxLen; i++ {
		argsHolderArray = append(argsHolderArray, "?")
	}
	for i := 0; i < (len(s.Args)/argMaxLen - 1); i++ {
		argsPlaceholder = append(argsPlaceholder, fmt.Sprintf("(%s)", strings.Join(argsHolderArray, ",")))
	}
	s.NewItem = fmt.Sprintf("%s,%s", s.NewItem, strings.Join(argsPlaceholder, ","))
}

func (sincr *SqlIncrementalStatement) Enqueue(args ...interface{}) {
	if len(sincr.Statements) == 0 {
		return
	}
	var payload = sincr.String()
	if foundNewIncrementalDataCallback != nil {
		foundNewIncrementalDataCallback(payload, args...)
	}
}

func (sincr *SqlIncrementalStatement) String() string {
	b, _ := json.Marshal(sincr)
	return string(b)
}

func (sincr *Statement) fixConditions(parametersCount int) {
	var (
		tempArgs = make([]string, len(sincr.Args))
	)
	copy(tempArgs, sincr.Args)
	sincr.Args = sincr.Args[:0]

	for j := 0; j < len(tempArgs)/parametersCount; j++ {
		var (
			temp = tempArgs[j*parametersCount : (j*parametersCount)+parametersCount]
		)
		for i := 0; i < len(temp); i++ {
			var current = temp[i]
			if shouldSpitParameters(current) {
				var (
					m = make([]string, len(temp))
				)
				copy(m, temp)
				for _, spf := range strings.Split(temp[i], ",") {
					if spf == "" {
						break
					}
					m[i] = spf
					sincr.Args = append(sincr.Args, m...)
				}
			}
		}
	}
	if len(sincr.Args) == 0 {
		sincr.Args = tempArgs
	}
	sincr.fixUpdateStmtPlaceholder(parametersCount)
}

func isInArray(str string, arr []string) bool {
	for _, v := range arr {
		if v == str {
			return true
		}
	}
	return false
}

func shouldSpitParameters(params string) bool {
	if strings.HasPrefix(params, "[") || strings.HasPrefix(params, "{") {
		return false
	}
	return strings.Contains(params, ",")
}
