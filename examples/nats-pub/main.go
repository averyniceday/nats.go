// Copyright 2012-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	json "encoding/json"
	"flag"
	"fmt"
	mpath "github.com/averyniceday/go-mpath-proto/mpath-api"
	"github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

// NOTE: Can test with demo servers.
// nats-pub -s demo.nats.io <subject> <msg>

func usage() {
	log.Printf("Usage: nats-pub [-s server] [-creds file] [-nkey file] [-tlscert file] [-tlskey file] [-tlscacert file] <subject> <msg>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func createMongoClient(hostname string) mongo.Client {
	clientOptions := options.Client().ApplyURI(hostname)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatalf("mongo.Connect() ERROR: %v", err)
	}
	return *client
}

func getCVRSamplesFromJSON(cvrJsonPath string) mpath.SomeMessage {
	jsonFile, err := os.Open(cvrJsonPath)
	defer jsonFile.Close()

	if err != nil {
		fmt.Println(err)
	}
	byteValue, _ := ioutil.ReadAll(jsonFile)
	cvrSamples := mpath.SomeMessage{}
	err = json.Unmarshal(byteValue, &cvrSamples)
	return cvrSamples
}


func getCVRSessionToken(getCVRUrl string, user string, password string) (*http.Request, context.CancelFunc, error) {
	CVRSessionUrl := getCVRUrl + "create_session" + "/" + user + "/" + password + "/0"
	fmt.Println("URL IS " + CVRSessionUrl)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, CVRSessionUrl, nil)
	if err != nil {
		return nil, cancel, err
	}
	req.Header.Set("Accept", "application/json")
	return req, cancel, nil
}

func getCVRSamplesFromWebService(user string, password string) ([]byte, error) {
	CVRUrl := "http://lynx.mskcc.org:9770/"
	req, cancel, err := getCVRSessionToken(CVRUrl, user, password)
	if err != nil {
		return nil, err
	}
	defer cancel()

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP return code != 200: %d\n", resp.StatusCode)
	}
	return body, err
}

func getLatestSampleInMongo(sampleId uint32, collection *mongo.Collection, ctx context.Context) *mpath.SomeMessage_Results {
	fmt.Println("getting Sample Document from Mongo")
	findOptions := options.Find()
	findOptions.SetSort(bson.D{{"dateAdded", -1}})
	filter := bson.D{{"metadata.cbxsampleid", sampleId}}
	var sample []mpath.SomeMessage_Results
	sampleResult, err := collection.Find(ctx, filter, findOptions)
	if err = sampleResult.All(ctx, &sample); err != nil {
		log.Fatal(err)
	}
	to_return := mpath.SomeMessage_Results{}
	if sample != nil {
		to_return = sample[0]
	}
	return &to_return
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var nkeyFile = flag.String("nkey", "", "NKey Seed File")
	var tlsClientCert = flag.String("tlscert", "", "TLS client certificate file")
	var tlsClientKey = flag.String("tlskey", "", "Private key file for client certificate")
	var tlsCACert = flag.String("tlscacert", "", "CA certificate to verify peer against")
	var reply = flag.String("reply", "", "Sets a specific reply subject")
	var showHelp = flag.Bool("h", false, "Show help message")
	var setup = flag.Bool("setup", false, "Insert records without updating")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 2 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Publisher")}

	if *userCreds != "" && *nkeyFile != "" {
		log.Fatal("specify -seed or -creds")
	}

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Use TLS client authentication
	if *tlsClientCert != "" && *tlsClientKey != "" {
		opts = append(opts, nats.ClientCert(*tlsClientCert, *tlsClientKey))
	}

	// Use specific CA certificate
	if *tlsCACert != "" {
		opts = append(opts, nats.RootCAs(*tlsCACert))
	}

	// Use Nkey authentication.
	if *nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(*nkeyFile)
		if err != nil {
			log.Fatal(err)
		}
		opts = append(opts, opt)
	}

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	getCVRSamplesFromWebService()
	os.Exit(5)
	mongoClient := createMongoClient("mongodb://localhost:27018")
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	col := mongoClient.Database("Processed_Samples").Collection("CVR_Results")

	cvrSamples := getCVRSamplesFromJSON("fetchjson.json")
	subj, msg := args[0], []byte(args[1])

	// for every  sample in the JSON
	for i:=0; i< len(cvrSamples.Results); i++ {
		if *setup {
			msg := []byte(cvrSamples.Results[i].MetaData.DmpSampleId + "added to the database")
			if reply != nil && *reply != "" {
				nc.PublishRequest(subj, *reply, msg)
			} else {
				nc.Publish(subj, msg)
			}
			nc.Flush()
		} else {
			// Step 1 - see if sample already exists inside mongo (sample w/o dateAdded field)
			// Step 2 - if sample does not exist, just add with new date
			// Step 3 - if samples exists in mongo - compare to what will be inserted
			// Step 4 - if different, insert with new date, else do nothing
			latestSample := getLatestSampleInMongo(cvrSamples.Results[i].MetaData.CbxSampleId, col, ctx)
			if latestSample != nil {
				// last sample is different from current one - add with new date
				if latestSample.String() != cvrSamples.Results[i].String() {
					result, err := col.InsertOne(ctx, cvrSamples.Results[i])
					fmt.Println(result)
					fmt.Println(err)
				} else {
					fmt.Println("This sample already exists, skipping...")
				}
			} else {
				fmt.Println("Hi")
			}

			// filter based on CbxSampleId
			opts := options.Update().SetUpsert(true)
			filter := bson.D{{"metadata.cbxsampleid", cvrSamples.Results[i].MetaData.CbxSampleId}}
			update := bson.D{{"$set", cvrSamples.Results[i]}}
			result, err := col.UpdateOne(
				context.TODO(),
				filter,
				update,
				opts,
			)
			// if updating an existing record we add a date
			if result.ModifiedCount == 1 {
				fmt.Println("Updated info for " + cvrSamples.Results[i].MetaData.DmpSampleId)
				filter := bson.D{{"metadata.cbxsampleid", cvrSamples.Results[i].MetaData.CbxSampleId}}
				update := bson.D{{"$set", bson.D{{"dateAdded", time.Now().String()}}}}
				col.UpdateOne(
					context.TODO(),
					filter,
					update,
					opts,
				)
			}
			// if no matched count it means it's a brand new record
			if result.MatchedCount == 0 {
				fmt.Println("No record existed, inserted record for " + cvrSamples.Results[i].MetaData.DmpSampleId)
			}

			if err != nil {
				// ErrNoDocuments means that the filter did not match any documents in
				// the collection.
				if err == mongo.ErrNoDocuments {
					return
				}
				log.Fatal(err)
			}
		}

	}

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Published [%s] : '%s'\n", subj, msg)
	}
}
