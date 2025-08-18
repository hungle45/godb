package godb

import (
	"fmt"
	"os"
)

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (int, error) {
	dumpFileName := "_lab1_dump.dat"
	_ = os.Remove(dumpFileName)

	hf, err := NewHeapFile(dumpFileName, &td, bp)
	if err != nil {
		return 0, fmt.Errorf("error creating heap file: %w", err)
	}

	csvFile, err := os.Open(fileName)
	if err != nil {
		return 0, fmt.Errorf("error opening CSV file: %w", err)
	}
	defer csvFile.Close()

	if err := hf.LoadFromCSV(csvFile, true, ",", false); err != nil {
		return 0, fmt.Errorf("error loading CSV file: %w", err)
	}

	fieldID, err := findFieldInTd(FieldType{Fname: sumField, Ftype: IntType}, &td)
	if err != nil {
		return 0, fmt.Errorf("error finding field in tuple descriptor: %w", err)
	}

	var sumResult int64
	iter, err := hf.Iterator(0)
	if err != nil {
		return 0, fmt.Errorf("error creating iterator: %w", err)
	}
	for {
		tup, err := iter()
		if err != nil {
			return 0, fmt.Errorf("error iterating tuples: %w", err)
		}
		if tup == nil {
			break
		}
		sumResult += tup.Fields[fieldID].(IntField).Value
	}

	return int(sumResult), nil
}
