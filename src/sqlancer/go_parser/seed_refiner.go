package main

import (
	//"fmt"
	//"time"
	//"math/rand"
	//"reflect"
	"strings"
	"os"
	"path/filepath"
	"bufio"
	"io"
	"regexp"
	"flag"


	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	//"database/sql"
	  "github.com/pingcap/tidb/pkg/parser/format"
        _ "github.com/pingcap/tidb/pkg/types/parser_driver"
	_ "github.com/go-sql-driver/mysql"
)
var dep map[string]string
var free_name []string
var isCreateStatement bool
var isQuery bool
var table_in_query []string
var ran_chosen_name string
var refine_res []string
var try_parse bool
var isForeignRef bool
var dbname = flag.String("dbname", "database0", "database name")
func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	//fmt.Println("parsing " + sql)
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil || stmtNodes == nil{
	//	fmt.Println(err)
		return nil, err
	}

	return &stmtNodes[0], nil
}
type colX struct{
	colNames []string
}
type refineVisitor struct{

}
func isSysTable(in *ast.TableName) bool{
	
	if strings.Contains(in.Name.L, "information_schema")||
	   strings.Contains(in.Schema.L, "information_schema")||
	   strings.Contains(in.Name.L, "opt_rule_blacklist") ||
	   strings.Contains(in.Schema.L, "opt_rule_blacklist"){
		in.Schema.O = in.Schema.L
		return true
	}else{
		return false
	}
}
func (v *refineVisitor) Enter(in ast.Node) (ast.Node, bool) {
	/*if name, ok := in.(*ast.ColumnName); ok {
		name.Name.O = dep["t0"][rand.Intn(2)]
	}*/
	if try_parse {
		return in, false
	}
	if _, ok := in.(*ast.SelectStmt); ok {
		isQuery = true
	}
	if name, ok := in.(*ast.TableName); ok {
		//name.Name.O = "t0"
		name.Schema.O = ""
		//fmt.Println("#######" + name.Schema.O)
	}
	if _, ok := in.(*ast.CreateTableStmt); ok {
		isCreateStatement = true
	}else if _, ok := in.(*ast.CreateSequenceStmt); ok {
		isCreateStatement = true
	}else if _, ok := in.(*ast.ReferenceDef); ok {
		isForeignRef = true;
	}else if _, ok := in.(*ast.FuncCallExpr); ok {
		isForeignRef = true;
	}
/*	if name, ok := in.(*ast.Constraint); ok {
		fmt.Println(name)
	}*/
	if name, ok := in.(*ast.TableName); ok {
	//	fmt.Println("schema is " + name.Schema.O)
	//	fmt.Println("schema is " + name.Schema.L)
	//	if name.Schema.O == "" {
	//		name.Schema.O = name.Schema.L
	//	}
		if isCreateStatement {
			if !isForeignRef {
				if !isSysTable(name) {
					dep[name.Name.O] = free_name[0]
					name.Name.O = free_name[0]
					free_name = free_name[1:]
				}
			} else {
				new_name, ok := dep[name.Name.O]
				if ok {
					name.Name.O = new_name
				} else {
					name.Name.O = ran_chosen_name
				}
			}
		} else {
			if !isSysTable(name) {
				new_name, ok := dep[name.Name.O]
				if ok {
					name.Name.O = new_name
				} else {
					name.Name.O = ran_chosen_name
				}
			}
 			if isQuery {
				table_in_query = append(table_in_query, name.Name.O)
		
			}
			
		}
		
	}

	//fmt.Println("type is ", reflect.TypeOf(in).String(), "value is ", in)

	return in, false
}
func (v *refineVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
type StringPairs struct{
	first []string
	second []string
}
type SeedPool struct{
	seedPool []StringPairs
}
func (pool *SeedPool) readTestcases(file string) {

}
func (pool *SeedPool) initSeedPool() {
	var files []string

	root := "../seeds"
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {

		ok := strings.HasSuffix(path, ".test")//testcases are end with .test
		if ok {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		pool.readTestcases(file)
	}

}
func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	/*if name, ok := in.(*ast.ColumnName); ok {
		name.Name.O = dep["t0"][rand.Intn(2)]
	}
	if name, ok := in.(*ast.TableName); ok {
		name.Name.O = "t0"
	}*/
	if _, ok := in.(*ast.CreateTableStmt); ok {
		isCreateStatement = true
	}

	//fmt.Println("type is ", reflect.TypeOf(in).String(), "value is ", in)

	return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
func extract(rootNode *ast.StmtNode) []string {
	v := &colX{}
	(*rootNode).Accept(v)
	return v.colNames
}
func refine_sql(sql string, free_name []string) string {
	isCreateStatement = false
	isForeignRef = false
	isQuery = false
	table_in_query = nil
	if len(strings.Trim(strings.Trim(sql, "\n"), "\r")) == 0 {//empty line
		return ""
	}
	astNode, err := parse(sql)

	if err != nil || astNode == nil{
		//fmt.Println(err)
		return ""
	}
	

	v := &refineVisitor{}
	(*astNode).Accept(v)
	//fmt.Println(*astNode)

	res := restoreSql(astNode)
	res += ";"
	if isQuery {
		res += "table_in_this_query:"
		for id, name := range table_in_query {
			if id > 0 {
				res += ","
			}
			res += name
		}
	}
	return res
}
func restoreSql(astNode *ast.StmtNode) string{
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreNameBackQuotes | format.RestoreStringWithoutCharset
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)
	errs := (*astNode).Restore(restoreCtx)
	if errs != nil {
		return ""
	}
	res := sb.String()
	for k, v := range dep {
		pattern := "`" + k + "`\\."
		reg := regexp.MustCompile(pattern)//replace fields like "tableName.column"
		//fmt.Println("replacing " + ".")
		res = reg.ReplaceAllString(res, " `" + v + "`.")
	}

	/*pattern := "`[a-zA-Z0-9]+`\\."//some table may not exist
	reg := regexp.MustCompile(pattern)
	res = reg.ReplaceAllString(res, " `" + ran_chosen_name + "`\\.")*/
	

	return  res
}
func refine_without_prefix(prefix string, str string) {
	idx := strings.Index(str, prefix) + len(prefix)
	//fmt.Println(str + "###" + str[idx:])
	tmp := refine_sql(str[idx:], free_name)
	if(tmp != "") {
		refine_res = append(refine_res, prefix + " " + tmp)
	}
}
func main() {
	
	flag.Parse();

	//fmt.Println("dbname is " + *dbname)
	dep = make(map[string]string)
	file, err := os.OpenFile("../../../tmp/seed_tmp_" + *dbname, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		//fmt.Println(err)
		return
	}
	parse_res_file, err := os.OpenFile("../../../tmp/seed_refine_res_" + *dbname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		//fmt.Println(err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var sql []string 
	//var expected_err []string
	flag := 0
	try_parse = true
	var str string
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		if string(line) == "$%#Seed$%#" {
			flag = 1
			continue
		}
		if string(line) == "$%#Name$%#" {
			flag = 2
			continue
		}
		if strings.HasPrefix(string(line), "##") {
			continue
		}
		//fmt.Println("read " + string(line))
		if flag == 2{
			free_name = append(free_name, string(line))
			ran_chosen_name = string(line)
			continue
		}else{
			cur_line := string(line)
			
			_, err = parse(cur_line)
	//		fmt.Println("cur_line is " + cur_line)
	//		fmt.Println(err)
	//		fmt.Println("str is " + str)
			if(err != nil){
				str += cur_line
				str += "\n"
			}else{
				if(strings.HasSuffix(str, ";")||strings.HasSuffix(str, ";\n")) {
					sql = append(sql, str)
					str = cur_line
					str += "\n"
				}else{
					str += cur_line
					str += "\n"
				}
			}
			/*str += string(line)
			if strings.Contains(string(line), "explain") {//special process for seeds contain 'explain'
				//fmt.Println("drop " + str)
				str = string(line)
				if flag == 1{
					sql = append(sql, str)
					str = ""
				}
				
			}else if strings.HasSuffix(string(line), ";") {
				if flag == 1{
					sql = append(sql, str)
				}
				str = ""
			}*//*else if strings.Contains(string(line), "ERROR")||strings.Contains(string(line), "Error") {
				pattern := "(?<=(Error|ERROR)[a-zA-Z0-9 ()]*:).+" //this pattern means: grab 'msg' in strings like "Error(123asd): msg"
				reg := regexp.MustCompile(pattern)
				expected_err = append(expected_err, reg.FindString(string(line)))
			}*/
		}
		
		

	}
	sql = append(sql, str)
	
	// for _,v := range sql {
	// 	fmt.Println(v)
	// }
	// fmt.Println("names:")
	// fmt.Println(free_name)
	try_parse = false
	
	for _, value := range sql {
		value = strings.Trim(value, " ")
		//fmt.Println(value)
		if(strings.HasPrefix(value, "revoke") || strings.HasPrefix(value, "REVOKE")){//special process for revoke statements
			refine_without_prefix(value[:6], value)
		}else if(strings.HasPrefix(value, "grant") || strings.HasPrefix(value, "GRANT")){
			refine_without_prefix(value[:5], value)
		}else if(strings.HasPrefix(value, "prepare") || strings.HasPrefix(value, "PREPARE")){//special process for prepare statements
			st := strings.IndexAny(value, "'") + 1
			en := strings.LastIndex(value, "'")
			//fmt.Println(value[:st])
			//fmt.Println(value[st:en])
			refine_res = append(refine_res, value[:st] + refine_sql(value[st:en], free_name) + "'")
		}else{
			refine_res = append(refine_res, refine_sql(value, free_name))
		}
		
	}

	//fmt.Println(refine_res)
	//fmt.Println(dep)
	//fmt.Println(expected_err)
	for _, value := range refine_res {
		if strings.Contains(value, "bdr") || strings.Contains(value, "BDR") {
			continue;
		}
		parse_res_file.WriteString(value + "\n")
	}




}

