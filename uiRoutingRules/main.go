package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)
const (
 headerString = "|%-5s|%-20s | %-20s | %-10s %-5s %10s | %-20s \n"

)
//==============================================================================
type (
	// Struc cac Rule generate tu JSON
	JsonRule struct {
		Rules []struct {
			FromTable string   `json:"fromTable"`
			ToTables  []string `json:"toTables"`
		} `json:"Rules"`
	}

	// Struct FROM trong JSON RULE
	FromTable struct {
		TableName            string
		DBName               string
		All, Replica, Rdonly bool
	}

	// Struct TO trong JSON RULE
	ToTable struct {
		TableName string
		DBName    string
	}
)

//====================================================

// Method khoi tao
func (re *FromTable) init(typeVar string) {
	re.All, re.Replica, re.Rdonly = false, false, false
	switch {
	case typeVar == "Replica":
		re.Replica = true
	case typeVar == "Rdonly":
		re.Rdonly = true
	default:
		re.All = true
	}
}

// Method de get Type tu 3 bien Bool
func (re FromTable) getType() string {
	switch {
	case re.Replica:
		return "Replica"
	case re.Rdonly:
		return "Rdonly"
	default:
		return ""
	}
}

//==============================================================================
type ObjectRule struct {
	From FromTable
	To   []ToTable
}

// Chuyen string From trong Json sang object: world_x.smallworld@replica => 3 var
func renderFromTable(inputString string) FromTable {
	switch {

	// Kiem tra tro theo dang chung chung smallworld (table name only)
	case !regexp.MustCompile(`\.|@`).MatchString(inputString):
		temp := FromTable{TableName: inputString}
		temp.init("")
		return temp

		// Kiem tra tro theo dang DatabaseName.TableName
	case regexp.MustCompile(`\.`).MatchString(inputString):
		temp := FromTable{DBName: strings.Split(inputString, ".")[0]}

		//Kiem tra co them theo type @replica
		if regexp.MustCompile(`@`).MatchString(strings.Split(inputString, ".")[1]) {
			temp.init(strings.Split(inputString, "@")[1])
			temp.TableName = strings.Split(strings.Split(inputString, "@")[0], ".")[1]
			return temp
		}
		//Neu khong return
			temp.TableName = strings.Split(inputString, ".")[1]
			return temp

	// Tro theo dang TableName@replica
	default:
		temp := FromTable{TableName: strings.Split(inputString, "@")[0]}
		temp.init(strings.Split(inputString, "@")[1])
		return temp
	}
}

func renderToTables(inputString string) ToTable {
	return ToTable{TableName: strings.Split(inputString, ".")[1], DBName: strings.Split(inputString, ".")[0]}
}

func debugFunc1(objectRules []ObjectRule) {
	fmt.Printf("\n\n\n\n\n")
	fmt.Println("----------------------------------------------------")
	fmt.Printf(  headerString, "ID", "Database", "Table", "Type", "", "To DB", "Table")
	fmt.Println("----------------------------------------------------")
	pre := ""
	for i, value := range objectRules {
		if pre != value.From.DBName {
			fmt.Println()
			fmt.Println(value.From.DBName)
			fmt.Println("-----------------------------------------------------------------------------------------------")
			pre = value.From.DBName
		} else {
			pre = value.From.DBName
		}
		fmt.Printf("|%-5d|%-20s | %-20s | %-10s ", i+1, value.From.DBName, value.From.TableName, value.From.getType())
		fmt.Printf("%5s %10s | %-20s\n", "==> ", value.To[0].DBName, value.To[0].TableName)
		fmt.Println("----------------------------------------------------")
	}
}

func debugFunc2(objectRules []ObjectRule) {
	fmt.Printf("\n\n\n\n\n")
	fmt.Println("----------------------------------------------------")
	fmt.Printf( headerString, "ID", "Database", "Table", "Type", "", "To DB", "Table")
	fmt.Println("----------------------------------------------------")
	pre := ""
	for i, value := range objectRules {
		if pre != value.To[0].DBName {
			fmt.Println("\n")
			fmt.Println(value.To[0].DBName)
			fmt.Println("-----------------------------------------------------------------------------------------------")
			pre = value.To[0].DBName
		} else {
			pre = value.To[0].DBName
		}
		fmt.Printf("|%-5d|%-20s | %-20s | %-10s ", i+1, value.From.DBName, value.From.TableName, value.From.getType())
		fmt.Printf("%5s %10s | %-20s\n", "==> ", value.To[0].DBName, value.To[0].TableName)
		fmt.Println("----------------------------------------------------")
	}
}

func sortBySource(objectRules []ObjectRule) {
	sort.Slice(objectRules, func(p, q int) bool {
		switch {
		case objectRules[p].From.DBName != objectRules[q].From.DBName:
			return objectRules[p].From.DBName < objectRules[q].From.DBName
		default:
			return objectRules[p].From.TableName < objectRules[q].From.TableName
		}
	})
}

func sortByDest(objectRules []ObjectRule) {
	//================ Grop by DB Name
	sort.Slice(objectRules, func(p, q int) bool {
		switch {
		case objectRules[p].To[0].DBName != objectRules[q].To[0].DBName:
			return objectRules[p].To[0].DBName < objectRules[q].To[0].DBName
		default:
			return objectRules[p].To[0].TableName < objectRules[q].To[0].TableName
		}
	})
}

//Render tu slice JSON sang Object
func renderToObject(jsonRules JsonRule, objectRules []ObjectRule) []ObjectRule {
	for i, val1 := range jsonRules.Rules {

		// Vong lap de chuyen nhieu TO vao Object
		listRenderedToTable := []ToTable{}
		for pos := range val1.ToTables {
			listRenderedToTable = append(listRenderedToTable, renderToTables(val1.ToTables[pos]))
		}

		// Chuyen 2 phan From & To  vao 1 Object sau do chuyen vao list
		anOjectbRule := ObjectRule{From: renderFromTable(jsonRules.Rules[i].FromTable), To: listRenderedToTable}
		objectRules = append(objectRules, anOjectbRule)
	}
	return objectRules
}

//Ham show menu va get lua chon menu
func getChoice() string {
	fmt.Println("Reading from: rule.txt...\n")
	fmt.Println("Showing sorted Rules by:")
	fmt.Println("\t1. Source DB and Table")
	fmt.Println("\t2. Destinaton DB and Table")
	fmt.Println("\t3. Finding Source DB or Table")
	fmt.Println("\t4. Finding Destinaton DB or Table")
	fmt.Println("\t5. Adding new JsonRule")
	fmt.Print("Your choice: ")
	inputString := ""
	fmt.Scanln(&inputString)
	return inputString
}

//==============================================================================

func main() {
	// Doc tu file
	bytes, _ := ioutil.ReadFile("rule.json")

	//Khai bao bien luu tru Json va bien de luu tru dang object
	var (
		jsonRules   JsonRule
		objectRules []ObjectRule
	)

	//Chuyen chuoi byte vao bien JSON
	_ = json.Unmarshal(bytes, &jsonRules)

	// Chuyen sang Object
	objectRules = renderToObject(jsonRules, objectRules)

	// Menu section
	inputString := getChoice()

	//Sort lan dau
	sortBySource(objectRules)

	//Cac chuc nang theo menu
	switch {
	case inputString == "2": // Sort theo dich den cua 1 Rule
		sortByDest(objectRules)
		debugFunc2(objectRules)
	case inputString == "5":
	default: // Sort theo source cua 1 rule
		debugFunc1(objectRules)
	}

}
