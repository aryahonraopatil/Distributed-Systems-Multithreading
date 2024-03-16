//aos project1 - multithreading application
//Arya Honraopatil 

package main

import (
	"os"
	"fmt"
	"strconv"
	"sync"
	"regexp"
	"strings"
	"math"
	"encoding/json"
)

//here we define the struct

type message struct {
	Fname string
	Start int
	End int
}

type worker struct {
	id int
	job chan []byte
	result chan []byte
}

type result struct{
	Value uint64
	Prefix string
	Suffix string
	Error string
}

//here we check for trailing whitespace and we replace it by space
func clean_chunk(chunk string) string{
	ts := regexp.MustCompile(`^[\s\p{Zs}]+|[\s\p{Zs}]+$`)
	chunk_ := ts.ReplaceAllString(chunk, " ")

	ds := regexp.MustCompile(`[\s\p{Zs}]{2,}`)
	chunk_ = ds.ReplaceAllString(chunk_, " ")

	return chunk_
}

//here we check the if there is any error(if a chunk of integer have neither suffix nor prefix) 
func error_check(e error){
	if e != nil{
		panic(e)
	}
}

//co-ordinator and worker functions

func coordinator(fname string, M float64){
	var resp = make([][]byte, int(M))
	var msg *message
	var frg map[string] interface{}
	var temp = ""
	var continue_ bool = false  //

	wg := new(sync.WaitGroup)

//channels to communicate between workers & coordinators
	job_ := make(chan []byte, int(M) * 10)
	result_ := make(chan []byte, int(M) * 10)

	sum := uint64(0)

	read, err := os.Open(fname)
	error_check(err)

	defer read.Close()

	fs, err :=  read.Stat()  //here we compute the file size

	frag := math.Floor(float64(fs.Size()-1) / M)  //we partition into M chunks

	last := int64(fs.Size()) - ( int64(frag) * int64(M) )

	fsize := 0

	fmt.Println("Total File Size: ", fs.Size())
	for i:=0; i < int(M); i++ {


		if i == int(M)-1 {
			msg = &message{
				fname,
				fsize,fsize + int(frag) + int(last) - 1,
			}
		} else {
			msg = &message{
				fname,
				fsize,
				fsize + int(frag) - 1,
			}
		}

		pckFile,_ := json.Marshal(msg)
		fmt.Printf("JSON sent to the Worker %d: %s\n", i, pckFile)
		job_ <- pckFile

		wrk := &worker{i, job_ , result_}

		go wrk.compute(wg)
		wg.Add(1)

		resp[i] = <-result_
		fsize += int(frag)
	}

	for index, frag := range resp{
		err := json.Unmarshal(frag, &frg)
		error_check(err)
		fmt.Printf("Worker %d responded with: Value(%v), Prefix(%v), Suffix(%v), Error(%v)\n",index,frg["Value"],frg["Prefix"],frg["Suffix"],frg["Error"] )

		if(frg["Error"].(string) == ""){
			if(frg["Suffix"].(string) != "" && frg["Prefix"].(string) != ""){
				temp = fmt.Sprintf("%s%s", temp, frg["Prefix"].(string))
				newValue,_ := strconv.ParseInt(temp,10,64)
				sum = sum + uint64(newValue)
				temp = frg["Suffix"].(string)
				continue_ = true

			} else if(frg["Suffix"].(string) != ""){
				fmt.Println("CASE 2", continue_)
				if(continue_ == false){
					temp = frg["Suffix"].(string)
					continue_ = true
				} else{
					a := fmt.Sprintf("%s%s", frg["Prefix"].(string), temp)
					res,_ := strconv.ParseUint(a, 10, 64)
					sum += res
					temp = frg["Suffix"].(string)
				}
			} else if(frg["Prefix"].(string) != ""){
				if(continue_ == false){

					newValue,_ := strconv.ParseInt(frg["Prefix"].(string),10,64)
					sum = sum + uint64(newValue)

				} else{
					temp = fmt.Sprintf("%s%s", temp, frg["Prefix"].(string))
					newValue,_ := strconv.ParseInt(temp,10,64)
					sum = sum + uint64(newValue)
					temp = ""
					continue_ = false
				}
			} else{
				continue_ = false
			}

		} else{
			if(continue_ == false) {

				temp = frg["Error"].(string)
				continue_ = true

			}else{
				temp = fmt.Sprintf("%s%s", temp, frg["Error"].(string))
			}

		}

		//here we calculate the sum
		if(temp != "" && continue_ == false) {
			newValue, _ := strconv.ParseInt(temp, 10, 64)
			fmt.Printf("When Temp-> sum + value + newvalue: %d + %d + %d\n", sum, uint64(frg["Value"].(float64)),uint64(newValue))
			sum = sum + uint64(frg["Value"].(float64)) +  uint64(newValue)
			temp = ""
			fmt.Printf(" sum + value: %d \n", sum)
		} else {
			fmt.Printf("When no Temp-> sum + value: %d + %d\n", sum, uint64(frg["Value"].(float64)))
			sum = sum + uint64(frg["Value"].(float64))
			fmt.Printf(" sum + value: %d \n", sum)
		}


	}
	er := json.Unmarshal(resp[len(resp)-1], &frg)
	error_check(er)
	temp = frg["Suffix"].(string)
	fmt.Println("Last Suffix", temp)
	newValue, _ := strconv.ParseUint(temp, 10, 64)
	sum = sum +  uint64(newValue)
    fmt.Printf("Total Sum is = %d", sum)
	wg.Wait()

}

func (wrkr *worker) compute(wg *sync.WaitGroup){
	var msg_ map[string]interface{}
	value := uint64(0)
	error := ""
	prefix := ""
	suffix := ""

	err := json.Unmarshal(<-wrkr.job, &msg_)
	error_check(err)

	//here we get the individual attributes from the fragment
	fname := msg_["Fname"].(string)
	start := msg_["Start"].(float64)
	end := msg_["End"].(float64)

	read, err := os.Open(fname)
	error_check(err)

	defer read.Close()

	_, e_ := read.Seek(int64(start),0 )
	error_check(e_)

	chunk := make([]byte, byte(end - start + 1))

	_, e2 := read.Read(chunk)
	error_check(e2)

	chunk_ := string(chunk)
	fmt.Printf("[%s]\n",chunk_)
	chunkStr := clean_chunk(chunk_)

	if(strings.Contains(chunkStr, " ")){
		psum := uint64(0)
		chunks := strings.Split(string(chunkStr), " ")
		prefix = chunks[0]     //if there is no starting or ending space, send it as preffix or suffix
		suffix = chunks[len(chunks) - 1]

		for _, element := range chunks[1:len(chunks) - 1]{

			val,_ := strconv.ParseInt(element, 10,64)
			psum += uint64(val)
		}

		value = psum   //here we get the partial sum

	} else{
		error = chunkStr   //if there is no partial sum then return in exception of json file
	}

	res := &result{
		value,
		prefix,
		suffix,
		error,
	}
	pckRes,_ := json.Marshal(res)

	wg.Done()

	wrkr.result <- pckRes

}

func main(){
	M := os.Args[1]
	fname := os.Args[2]
	frag, _ := strconv.ParseFloat(M, 64) //here we convert the string to float

	coordinator(fname, frag) //we pass the values to cordinator


}
