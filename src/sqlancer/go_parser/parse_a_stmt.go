package main

import (
	"fmt"
	"reflect"
	//"time"
	//"math/rand"
	//"reflect"
	"encoding/json"
	"strings"

	//"os"
	"bytes"
	//"path/filepath"
	//"bufio"
	//"io"
	//"regexp"
	"flag"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"

	//"database/sql"
	//  "github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

var dbname = flag.String("dbname", "database0", "database name")
var originalStmt = flag.String("original-stmt", "", "stmt to parse")
var encodedStmt EncodedStmt

type EncodedStmt struct {
	Stmt         string   `json:"stmt"` // json 标签指定 key
	StmtType     int      `json:"stmtType"`
	Tables       []string `json:"tables"`
	Cols         []string `json:"cols"`
	ColsType     []string `json:"colsType"`
	ParseSuccess bool     `json:"parseSuccess"`
}

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	//fmt.Println("parsing " + sql)
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil || stmtNodes == nil {
		//	fmt.Println(err)
		return nil, err
	}

	return &stmtNodes[0], nil
}

type parseVisitor struct {
}

func isSysTable(in *ast.TableName) bool {

	if strings.Contains(in.Name.L, "information_schema") ||
		strings.Contains(in.Schema.L, "information_schema") ||
		strings.Contains(in.Name.L, "opt_rule_blacklist") ||
		strings.Contains(in.Schema.L, "opt_rule_blacklist") {
		in.Schema.O = in.Schema.L
		return true
	} else {
		return false
	}
}
func (v *parseVisitor) Enter(in ast.Node) (ast.Node, bool) {

	// if name, ok := in.(*ast.ColumnName); ok {
	// 	encodedStmt.Cols = append(encodedStmt.Cols, name.Name.O)
	// }
	if node, ok := in.(*ast.ColumnDef); ok {
		//fmt.Println("col name is " + node.Name.Name.O + " col type is " + node.Tp.String())
		encodedStmt.Cols = append(encodedStmt.Cols, node.Name.Name.O)
		encodedStmt.ColsType = append(encodedStmt.ColsType, node.Tp.String())
	}
	if _, ok := in.(*ast.SelectStmt); ok {
		encodedStmt.StmtType = 2
	}
	if name, ok := in.(*ast.TableName); ok {
		encodedStmt.Tables = append(encodedStmt.Tables, name.Name.O)
	}
	if _, ok := in.(*ast.CreateTableStmt); ok {
		encodedStmt.StmtType = 0
	} else if _, ok := in.(*ast.CreateSequenceStmt); ok {
		encodedStmt.StmtType = 0
	}
	fmt.Println("type is ", reflect.TypeOf(in).String(), "value is ", in)
	return in, false
}
func (v *parseVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func parseStmt() {
	if len(strings.Trim(strings.Trim(encodedStmt.Stmt, "\n"), "\r")) == 0 { //empty line
		encodedStmt.ParseSuccess = false
		return
	}
	astNode, err := parse(*originalStmt)

	if err != nil || astNode == nil {
		//fmt.Println(err)
		encodedStmt.ParseSuccess = false
		return
	}

	v := &parseVisitor{}
	(*astNode).Accept(v)
}
func main() {
	//fmt.Println("success1")
	flag.Parse()

	// parse_res_file, err := os.OpenFile("../../../tmp/stmt_parse_res_" + *dbname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	// if err != nil {
	// 	//fmt.Println(err)
	// 	return
	// }
	fmt.Println(*originalStmt)
	encodedStmt.Stmt = strings.Trim(*originalStmt, " ")
	encodedStmt.StmtType = 1
	encodedStmt.ParseSuccess = true

	parseStmt()

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false) // 关闭 HTML 转义

	// jsonObject, err := json.Marshal(&encodedStmt)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	if err := encoder.Encode(encodedStmt); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Print(buf.String()) //the result must by last one to be print
	//parse_res_file.WriteString(buf.String() + "\n")
	// fmt.Println(string(jsonObject))
	// parse_res_file.WriteString(string(jsonObject) + "\n")

}
