package main

import (
	"fmt"
	"time"
	"math/rand"
	"reflect"
	"strings"
	"encoding/json"
	"os"
	//"path/filepath"
	//"bufio"
	//"io"
	//"regexp"
	//"flag"



	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	//"database/sql"
	  "github.com/pingcap/tidb/pkg/parser/format"
        _ "github.com/pingcap/tidb/pkg/types/parser_driver"
	_ "github.com/go-sql-driver/mysql"
)
var transformMaps map[string]string
func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil || stmtNodes == nil{
		return nil, err
	}

	return &stmtNodes[0], nil
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
	if name, ok := in.(*ast.ColumnName); ok {
		fmt.Println("refining " + name.Name.O)
		if value, exists := transformMaps[name.Table.O + "." + name.Name.O]; exists {
			name.Table.O = strings.Split(value, ".")[0]
			name.Name.O = strings.Split(value, ".")[1]
		}else{
			
			if name.Table.O != "" {
				var table string
				if value, exists := transformMaps[name.Table.O]; exists {
					table = value
				}else{
					table = tablesAndColumns.Tables[rand.Intn(len(tcmaps.Maps))]
				}
				cols := tcmaps.Maps[table]
				col := cols[rand.Intn(len(cols))]
				transformMaps[name.Table.O + "." + name.Name.O] = table + "." + col
				transformMaps[name.Table.O ] = table
				name.Table.O = table
				name.Name.O = col
			}else {
				table := tablesAndColumns.Tables[rand.Intn(len(tcmaps.Maps))]
				cols := tcmaps.Maps[table]
				col := cols[rand.Intn(len(cols))]
				transformMaps[name.Table.O + "." + name.Name.O] = "." + col
				name.Name.O = col
			}

		}
		
	}

	// if _, ok := in.(*ast.SelectStmt); ok {
		
	// }
	// if name, ok := in.(*ast.TableName); ok {
	// 	//name.Name.O = "t0"
	// 	name.Schema.O = ""
	// 	//fmt.Println("#######" + name.Schema.O)
	// }

/*	if name, ok := in.(*ast.Constraint); ok {
		fmt.Println(name)
	}*/
	if name, ok := in.(*ast.TableName); ok {
	//	fmt.Println("schema is " + name.Schema.O)
	//	fmt.Println("schema is " + name.Schema.L)
	//	if name.Schema.O == "" {
	//		name.Schema.O = name.Schema.L
	//	}
		fmt.Println("refining " + name.Name.O)
		if value, exists := transformMaps[name.Name.O]; exists {
			name.Name.O = value
			name.Name.L = value
		}else{
			table := tablesAndColumns.Tables[rand.Intn(len(tcmaps.Maps))]
			transformMaps[name.Name.O] = table
			name.Name.O = table
			name.Name.L = table
			
		}
		
		
	}

	fmt.Println("type is ", reflect.TypeOf(in).String(), "value is ", in)

	return in, false
}
func (v *refineVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
type StringPairs struct{
	first []string
	second []string
}

func refineSQL(sql string) string {

	if len(strings.Trim(strings.Trim(sql, "\n"), "\r")) == 0 {//empty line
		return ""
	}
	astNode, err := parse(sql)

	if err != nil || astNode == nil{
		return ""
	}
	

	v := &refineVisitor{}
	(*astNode).Accept(v)
	//fmt.Println(*astNode)

	res := restoreSql(astNode)

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
	// for k, v := range dep {
	// 	pattern := "`" + k + "`\\."
	// 	reg := regexp.MustCompile(pattern)//replace fields like "tableName.column"
	// 	//fmt.Println("replacing " + ".")
	// 	res = reg.ReplaceAllString(res, " `" + v + "`.")
	// }

	/*pattern := "`[a-zA-Z0-9]+`\\."//some table may not exist
	reg := regexp.MustCompile(pattern)
	res = reg.ReplaceAllString(res, " `" + ran_chosen_name + "`\\.")*/
	

	return  res
}
// func refine_without_prefix(prefix string, str string) {
// 	idx := strings.Index(str, prefix) + len(prefix)
// 	//fmt.Println(str + "###" + str[idx:])
// 	tmp := refine_sql(str[idx:], free_name)
// 	if(tmp != "") {
// 		refine_res = append(refine_res, prefix + " " + tmp)
// 	}
// }
type CmdArgs struct{
	Tables []string `json:"tables"`
	Columns [][]string `json:"columns"`
}
type TCMaps struct{//Map of tables and columns
	Maps map[string][]string
}
var tcmaps TCMaps
var tablesAndColumns CmdArgs
var stmt string
func main() {
	
	rand.Seed(time.Now().UnixNano())
	jsonStr := os.Args[1]
	stmt = os.Args[2]
	tcmaps.Maps = make(map[string][]string)
	transformMaps = make(map[string]string)
    
    err := json.Unmarshal([]byte(jsonStr), &tablesAndColumns)
    if err != nil {
        fmt.Printf("Error parsing JSON: %v\n", err)
        return
    }

    fmt.Printf("Config: %+v\n", tablesAndColumns)
	for _, value := range tablesAndColumns.Tables {

		for _, value2 := range tablesAndColumns.Columns {
			for _, col := range value2 {
				tcmaps.Maps[value] = append(tcmaps.Maps[value], col)
			}
		}
	}
	fmt.Println(stmt)
	fmt.Println(refineSQL(stmt))

	// flag.Parse();

	// //fmt.Println("dbname is " + *dbname)
	// dep = make(map[string]string)
	// file, err := os.OpenFile("../../../tmp/seed_tmp_" + *dbname, os.O_RDWR|os.O_CREATE, 0777)
	// if err != nil {
	// 	//fmt.Println(err)
	// 	return
	// }
	// parse_res_file, err := os.OpenFile("../../../tmp/seed_refine_res_" + *dbname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	// if err != nil {
	// 	//fmt.Println(err)
	// 	return
	// }
	// defer file.Close()

	// reader := bufio.NewReader(file)

	// var sql []string 
	// //var expected_err []string
	// flag := 0
	// try_parse = true
	// var str string
	// for {
	// 	line, _, err := reader.ReadLine()
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	if string(line) == "$%#Seed$%#" {
	// 		flag = 1
	// 		continue
	// 	}
	// 	if string(line) == "$%#Name$%#" {
	// 		flag = 2
	// 		continue
	// 	}
	// 	if strings.HasPrefix(string(line), "##") {
	// 		continue
	// 	}
	// 	//fmt.Println("read " + string(line))
	// 	if flag == 2{
	// 		free_name = append(free_name, string(line))
	// 		ran_chosen_name = string(line)
	// 		continue
	// 	}else{
	// 		cur_line := string(line)
			
	// 		_, err = parse(cur_line)
	// //		fmt.Println("cur_line is " + cur_line)
	// //		fmt.Println(err)
	// //		fmt.Println("str is " + str)
	// 		if(err != nil){
	// 			str += cur_line
	// 			str += "\n"
	// 		}else{
	// 			if(strings.HasSuffix(str, ";")||strings.HasSuffix(str, ";\n")) {
	// 				sql = append(sql, str)
	// 				str = cur_line
	// 				str += "\n"
	// 			}else{
	// 				str += cur_line
	// 				str += "\n"
	// 			}
	// 		}
	// 		/*str += string(line)
	// 		if strings.Contains(string(line), "explain") {//special process for seeds contain 'explain'
	// 			//fmt.Println("drop " + str)
	// 			str = string(line)
	// 			if flag == 1{
	// 				sql = append(sql, str)
	// 				str = ""
	// 			}
				
	// 		}else if strings.HasSuffix(string(line), ";") {
	// 			if flag == 1{
	// 				sql = append(sql, str)
	// 			}
	// 			str = ""
	// 		}*//*else if strings.Contains(string(line), "ERROR")||strings.Contains(string(line), "Error") {
	// 			pattern := "(?<=(Error|ERROR)[a-zA-Z0-9 ()]*:).+" //this pattern means: grab 'msg' in strings like "Error(123asd): msg"
	// 			reg := regexp.MustCompile(pattern)
	// 			expected_err = append(expected_err, reg.FindString(string(line)))
	// 		}*/
	// 	}
		
		

	// }
	// sql = append(sql, str)
	
	// // for _,v := range sql {
	// // 	fmt.Println(v)
	// // }
	// // fmt.Println("names:")
	// // fmt.Println(free_name)
	// try_parse = false
	
	// for _, value := range sql {
	// 	value = strings.Trim(value, " ")
	// 	//fmt.Println(value)
	// 	if(strings.HasPrefix(value, "revoke") || strings.HasPrefix(value, "REVOKE")){//special process for revoke statements
	// 		refine_without_prefix(value[:6], value)
	// 	}else if(strings.HasPrefix(value, "grant") || strings.HasPrefix(value, "GRANT")){
	// 		refine_without_prefix(value[:5], value)
	// 	}else if(strings.HasPrefix(value, "prepare") || strings.HasPrefix(value, "PREPARE")){//special process for prepare statements
	// 		st := strings.IndexAny(value, "'") + 1
	// 		en := strings.LastIndex(value, "'")
	// 		//fmt.Println(value[:st])
	// 		//fmt.Println(value[st:en])
	// 		refine_res = append(refine_res, value[:st] + refine_sql(value[st:en], free_name) + "'")
	// 	}else{
	// 		refine_res = append(refine_res, refine_sql(value, free_name))
	// 	}
		
	// }

	// //fmt.Println(refine_res)
	// //fmt.Println(dep)
	// //fmt.Println(expected_err)
	// for _, value := range refine_res {
	// 	if strings.Contains(value, "bdr") || strings.Contains(value, "BDR") {
	// 		continue;
	// 	}
	// 	parse_res_file.WriteString(value + "\n")
	// }




}

