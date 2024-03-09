package main

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// SimpleAsset implements a simple chaincode to manage an asset
type SimpleAsset struct {
}

// Init is called during chaincode instantiation to initialize any
// data. Note that chaincode upgrade also calls this function to reset
// or to migrate data.
func (t *SimpleAsset) Init(stub shim.ChaincodeStubInterface) pb.Response {
	value, err := set(stub, []string{"key1", "{\"value\":\"initial value\"}"})
	if err != nil {
		return shim.Error(fmt.Sprintf("failed to create asset: %s", value))
	}
	return shim.Success(nil)
}

// Invoke is called per transaction on the chaincode. Each transaction is
// either a 'get' or a 'set' on the asset created by Init function. The Set
// method may create a new asset by specifying a new key-value pair.
func (t *SimpleAsset) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	// Extract the function and args from the transaction proposal
	_, args := stub.GetFunctionAndParameters()

	var result string
	var err error

	if args[0] == "set" {
		result, err = set(stub, args[1:])
	} else if args[0] == "get" {
		result, err = get(stub, args[1:])
	} else if args[0] == "getList" {
		result, err = getList(stub, args[1:])
	} else {
		return shim.Error("invalid function name. Expecting 'set' or 'get' or 'getList', got: " + args[0])
	}
	if err != nil {
		return shim.Error(err.Error())
	}

	// Return the result as success payload
	return shim.Success([]byte(result))
}

// Set stores the asset (both key and value) on the ledger. If the key exists,
// it will override the value with the new one
func set(stub shim.ChaincodeStubInterface, args []string) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("incorrect arguments. Expecting a key and a value. Got: %v", args)
	}

	var err error

	// Unmarshal the value to check if it is a valid JSON
	var js map[string]interface{}
	err = json.Unmarshal([]byte(args[1]), &js)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal asset: %s", args[0])
	}

	// Write the state to the ledger
	err = stub.PutState(args[0], []byte(args[1]))
	if err != nil {
		return "", fmt.Errorf("failed to set asset: %s", args[0])
	}
	return args[1], nil
}

// Get returns the value of the specified asset key
func get(stub shim.ChaincodeStubInterface, args []string) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("incorrect arguments. Expecting a key. Got: %v", args)
	}

	value, err := stub.GetState(args[0])
	if err != nil {
		return "", fmt.Errorf("failed to get asset: %s with error: %s", args[0], err)
	}
	if value == nil {
		return "", fmt.Errorf("asset not found: %s", args[0])
	}

	var result = string(value)

	return result, nil
}

// Get returns a list of values of the specified asset keys
func getList(stub shim.ChaincodeStubInterface, args []string) (string, error) {
	if len(args) < 1 {
		return "", fmt.Errorf("incorrect arguments. Expecting a list of keys. Got: %v", args)
	}

	var result string
	for i := 0; i < len(args); i++ {
		value, err := stub.GetState(args[i])
		if err != nil {
			return "", fmt.Errorf("failed to get asset: %s with error: %s", args[i], err)
		}
		if value == nil {
			return "", fmt.Errorf("asset not found: %s", args[i])
		}

		result += string(value)

		// If it's not the last element, add a comma
		if i < len(args)-1 {
			result += ","
		}
	}

	// enclose result in square brackets
	if len(args) > 1 {
		result = "[" + result + "]"
	}

	return result, nil
}

// main function starts up the chaincode in the container during instantiate
//
//lint:ignore U1000 main is required
func main() {
	if err := shim.Start(new(SimpleAsset)); err != nil {
		fmt.Printf("Error starting SimpleAsset chaincode: %s", err)
	}
}
