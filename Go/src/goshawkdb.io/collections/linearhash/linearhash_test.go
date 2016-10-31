package linearhash

import (
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
	"math/rand"
	"testing"
	"time"
)

func createEmpty(th *tests.TestHelper) *LHash {
	c0 := th.CreateConnections(1)[0]
	lh, err := NewEmptyLHash(c0.Connection)
	if err != nil {
		th.Fatal(err)
		return nil
	}
	return lh
}

func assertSize(th *tests.TestHelper, lh *LHash, expected int64) {
	size, err := lh.Size()
	if err != nil {
		th.Fatal(err)
	} else if size != expected {
		th.Fatal(fmt.Sprintf("Expected to have %v size. Got %v", expected, size))
	}
}

func TestCreateNew(t *testing.T) {
	th := tests.NewTestHelper(t)
	defer th.Shutdown()

	lh := createEmpty(th)
	assertSize(th, lh, 0)

	lh2 := LHashFromObj(lh.Conn, lh.ObjRef)
	assertSize(th, lh2, 0)
}

func TestPutGetForEach(t *testing.T) {
	th := tests.NewTestHelper(t)
	defer th.Shutdown()

	lh := createEmpty(th)
	c0 := lh.Conn

	objCount := int64(1024)
	// create objs
	res, _, err := c0.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		result := make(map[string]client.ObjectRef)
		for idx := int64(0); idx < objCount; idx++ {
			str := fmt.Sprintf("%v", idx)
			objRef, err := txn.CreateObject(([]byte)(str))
			if err != nil {
				return nil, err
			}
			result[str] = objRef
		}
		return result, nil
	})
	if err != nil {
		th.Fatal(err)
	}

	objs := res.(map[string]client.ObjectRef)
	start := time.Now()
	_, _, err = c0.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		for str, objRefOld := range objs {
			objRef, err := txn.GetObject(objRefOld)
			if err != nil {
				return nil, err
			}
			th.Logf("Putting %v -> %v", str, objRef)
			err = lh.Put(([]byte)(str), objRef)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		th.Fatal(err)
	}
	assertSize(th, lh, objCount)

	mid := time.Now()
	for str, objRef := range objs {
		objRefFound, err := lh.Find(([]byte)(str))
		if err != nil {
			th.Fatal(err)
		}
		if objRefFound == nil {
			th.Fatal(fmt.Sprintf("Failed to find entry for %v", str))
		} else if !objRefFound.ReferencesSameAs(objRef) {
			th.Fatal(fmt.Sprintf("Entry for %v has value in %v instead of %v", str, objRefFound, objRef))
		}
	}
	assertSize(th, lh, objCount)
	end := time.Now()

	_, _, err = lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		th.Log("foreach restart")
		objsItr := make(map[string]bool, len(objs))
		err := lh.ForEach(func(key []byte, objRef client.ObjectRef) error {
			str := string(key)
			th.Log("ForEach yields key", str)
			if objsItr[str] {
				return fmt.Errorf("ForEach yielded key %v twice!", str)
			}
			objsItr[str] = true
			ref, found := objs[str]
			if !found {
				return fmt.Errorf("ForEach yielded unknown key: %v", str)
			}
			if !objRef.ReferencesSameAs(ref) {
				return fmt.Errorf("ForEach yielded unexpected value for key: %v (expected %v; actual %v)", str, ref, objRef)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(objsItr) != len(objs) {
			return nil, fmt.Errorf("ForEach yielded incorrect number of values: %v vs %v", len(objsItr), len(objs))
		}
		return nil, nil
	})
	if err != nil {
		th.Fatal(err)
	}
	forEachEnd := time.Now()

	th.Logf("Inserting: %v; fetching: %v; forEach: %v", mid.Sub(start), end.Sub(mid), forEachEnd.Sub(end))
}

func TestSoak(t *testing.T) {
	// Sadly undirected, but nevertheless fairly sensible way of doing
	// testing.
	th := tests.NewTestHelper(t)
	defer th.Shutdown()
	lh := createEmpty(th)

	seed := time.Now().UnixNano()
	// seed = int64(1475936141644630799)
	th.Logf("Seed: %v", seed)
	rng := rand.New(rand.NewSource(seed))
	// we use contents to mirror the state of the LHash
	contents := make(map[string]string)

	var err error
	for i := 4096; i > 0; i-- {
		lenContents := len(contents)
		// we bias creation of new keys by 999 with 1 more for reset
		op := rng.Intn((3*lenContents)+1000) - 1000
		opClass := 0
		opArg := 0
		if lenContents > 0 {
			opClass = op / lenContents
			opArg = op % lenContents
		}
		switch {
		case op == -1: // reset
			lh, err = NewEmptyLHash(lh.Conn)
			if err != nil {
				th.Fatal(err)
				return
			}
			contents = make(map[string]string)
			th.Log("NewLHash")

		case op < -1: // add new key
			key := fmt.Sprintf("%v", lenContents)
			value := fmt.Sprintf("Hello%v-%v", i, key)
			_, _, err = lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				valueObj, err := txn.CreateObject([]byte(value))
				if err != nil {
					return nil, err
				}
				return nil, lh.Put([]byte(key), valueObj)
			})
			if err != nil {
				th.Fatal(err)
			}
			contents[key] = value
			th.Logf("Put(%v, %v)", key, value)

		case opClass == 0: // find key
			key := fmt.Sprintf("%v", opArg)
			value := contents[key]
			inContents := len(value) != 0
			th.Logf("Find(%v) == %v ? %v", key, value, inContents)
			result, _, err := lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				valueObj, err := lh.Find([]byte(key))
				if err != nil {
					return nil, err
				}
				if valueObj == nil {
					return nil, nil
				} else {
					return valueObj.Value()
				}
			})
			if err != nil {
				th.Fatal(err)
			}
			if s, ok := result.([]byte); inContents && (result == nil || !ok || string(s) != value) {
				th.Fatalf("%v Failed to retrieve string value: %v", key, result)
			} else if !inContents && result != nil {
				th.Fatalf("Got result even after remove: %v", result)
			}

		case opClass == 1: // remove key
			key := fmt.Sprintf("%v", opArg)
			inContents := len(contents[key]) != 0
			th.Logf("Remove(%v) ? %v", key, inContents)
			err = lh.Remove([]byte(key))
			if err != nil {
				th.Fatal(err)
			}
			if inContents {
				contents[key] = ""
			}

		case opClass == 2: // re-put existing key
			key := fmt.Sprintf("%v", opArg)
			value := contents[key]
			inContents := len(value) != 0
			if !inContents {
				value = fmt.Sprintf("Hello%v-%v", i, key)
				contents[key] = value
			}
			_, _, err = lh.Conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				valueObj, err := txn.CreateObject([]byte(value))
				if err != nil {
					return nil, err
				}
				return nil, lh.Put([]byte(key), valueObj)
			})
			if err != nil {
				th.Fatal(err)
			}
			th.Logf("Put(%v, %v) ? %v", key, value, inContents)

		default:
			th.Fatalf("Unexpected op %v (class: %v; arg %v)", op, opClass, opArg)
		}
	}
}
