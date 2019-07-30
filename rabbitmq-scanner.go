package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"net/http"
        "net"
	"os"
	vault "github.com/akkeris/vault-client"
	"strconv"
	"time"
)

type RabbitMQQueue struct {
	ConsumerDetails []interface{} `json:"consumer_details"`
	Incoming        []interface{} `json:"incoming"`
	Deliveries      []interface{} `json:"deliveries"`
	MessagesDetails struct {
		Rate float64 `json:"rate"`
	} `json:"messages_details"`
	Messages                      int `json:"messages"`
	MessagesUnacknowledgedDetails struct {
		Rate float64 `json:"rate"`
	} `json:"messages_unacknowledged_details"`
	MessagesUnacknowledged int `json:"messages_unacknowledged"`
	MessagesReadyDetails   struct {
		Rate float64 `json:"rate"`
	} `json:"messages_ready_details"`
	MessagesReady     int `json:"messages_ready"`
	ReductionsDetails struct {
		Rate float64 `json:"rate"`
	} `json:"reductions_details"`
	Reductions   int `json:"reductions"`
	MessageStats struct {
		DeliverGetDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_get_details"`
		DeliverGet int `json:"deliver_get"`
		AckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"ack_details"`
		Ack              int `json:"ack"`
		RedeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"redeliver_details"`
		Redeliver           int `json:"redeliver"`
		DeliverNoAckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_no_ack_details"`
		DeliverNoAck   int `json:"deliver_no_ack"`
		DeliverDetails struct {
			Rate float64 `json:"rate"`
		} `json:"deliver_details"`
		Deliver         int `json:"deliver"`
		GetNoAckDetails struct {
			Rate float64 `json:"rate"`
		} `json:"get_no_ack_details"`
		GetNoAck   int `json:"get_no_ack"`
		GetDetails struct {
			Rate float64 `json:"rate"`
		} `json:"get_details"`
		Get            int `json:"get"`
		PublishDetails struct {
			Rate float64 `json:"rate"`
		} `json:"publish_details"`
		Publish int `json:"publish"`
	} `json:"message_stats"`
	Node      string `json:"node"`
	Arguments struct {
	} `json:"arguments"`
	Exclusive            bool   `json:"exclusive"`
	AutoDelete           bool   `json:"auto_delete"`
	Durable              bool   `json:"durable"`
	Vhost                string `json:"vhost"`
	Name                 string `json:"name"`
	MessageBytesPagedOut int    `json:"message_bytes_paged_out"`
	MessagesPagedOut     int    `json:"messages_paged_out"`
	BackingQueueStatus   struct {
		Mode              string        `json:"mode"`
		Q1                int           `json:"q1"`
		Q2                int           `json:"q2"`
		Delta             []interface{} `json:"delta"`
		Q3                int           `json:"q3"`
		Q4                int           `json:"q4"`
		Len               int           `json:"len"`
		NextSeqID         int           `json:"next_seq_id"`
		AvgIngressRate    float64       `json:"avg_ingress_rate"`
		AvgEgressRate     float64       `json:"avg_egress_rate"`
		AvgAckIngressRate float64       `json:"avg_ack_ingress_rate"`
		AvgAckEgressRate  float64       `json:"avg_ack_egress_rate"`
		MirrorSeen        int           `json:"mirror_seen"`
		MirrorSenders     int           `json:"mirror_senders"`
	} `json:"backing_queue_status"`
	HeadMessageTimestamp       interface{} `json:"head_message_timestamp"`
	MessageBytesPersistent     int         `json:"message_bytes_persistent"`
	MessageBytesRAM            int         `json:"message_bytes_ram"`
	MessageBytesUnacknowledged int         `json:"message_bytes_unacknowledged"`
	MessageBytesReady          int         `json:"message_bytes_ready"`
	MessageBytes               int         `json:"message_bytes"`
	MessagesPersistent         int         `json:"messages_persistent"`
	MessagesUnacknowledgedRAM  int         `json:"messages_unacknowledged_ram"`
	MessagesReadyRAM           int         `json:"messages_ready_ram"`
	MessagesRAM                int         `json:"messages_ram"`
	GarbageCollection          struct {
		MinorGcs        int `json:"minor_gcs"`
		FullsweepAfter  int `json:"fullsweep_after"`
		MinHeapSize     int `json:"min_heap_size"`
		MinBinVheapSize int `json:"min_bin_vheap_size"`
	} `json:"garbage_collection"`
	State                  string      `json:"state"`
	RecoverableSlaves      []string    `json:"recoverable_slaves"`
	SynchronisedSlaveNodes []string    `json:"synchronised_slave_nodes"`
	SlaveNodes             []string    `json:"slave_nodes"`
	Memory                 int         `json:"memory"`
	ConsumerUtilisation    interface{} `json:"consumer_utilisation"`
	Consumers              int         `json:"consumers"`
	ExclusiveConsumerTag   interface{} `json:"exclusive_consumer_tag"`
	Policy                 string      `json:"policy"`
}

var liveapi string
var liveuser string
var livepassword string
var sandboxapi string
var sandboxuser string
var sandboxpassword string
var brokerdb string
var pitdb string
var appspacelist map[string]string
var tsdbconn net.Conn
func main() {
	setCreds()
	appspaces, err := getAppSpaceList()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
        fmt.Println(appspaces)
        tsdbconn, err = net.Dial("tcp", os.Getenv("OPENTSDB_IP"))
        if err != nil {
                fmt.Println("dial error:", err)
                os.Exit(1)
        }
	getRabbitmqMetrics(appspaces)
        tsdbconn.Close()
}

func setCreds() {
	adminsecret := os.Getenv("RABBITMQ_ADMIN_SECRET")
	livesecret := os.Getenv("RABBITMQ_LIVE_SECRET")
	sandboxsecret := os.Getenv("RABBITMQ_SANDBOX_SECRET")
	pitdbsecret := os.Getenv("PITDB_SECRET")
	brokerdb = vault.GetField(adminsecret, "brokerdb")
	liveapi = vault.GetField(livesecret, "url")
	liveuser = vault.GetField(livesecret, "username")
	livepassword = vault.GetField(livesecret, "password")
	sandboxapi = vault.GetField(sandboxsecret, "url")
	sandboxuser = vault.GetField(sandboxsecret, "username")
	sandboxpassword = vault.GetField(sandboxsecret, "password")
	pitdb = vault.GetField(pitdbsecret, "location")

}

func getAppSpaceList() (l []string, e error) {
        var list []string

        brokerdb := os.Getenv("LISTDB")
        uri := brokerdb
        db, dberr := sql.Open("postgres", uri)
        if dberr != nil {
                fmt.Println(dberr)
                return nil, dberr
        }
        stmt,dberr := db.Prepare("select apps.name as appname, spaces.name as space from apps, spaces, services, service_attachments where services.service=service_attachments.service and owned=true and addon_name like '%rabbitmq%' and services.deleted=false and service_attachments.deleted=false and service_attachments.app=apps.app and spaces.space=apps.space;")
        defer stmt.Close()
        rows, err := stmt.Query()
        if dberr != nil {
                db.Close()
                fmt.Println(dberr)
                return nil, dberr
        }
        defer rows.Close()
        var appname string
        var space string
        
        for rows.Next() {
                err := rows.Scan(&appname, &space)
                if err != nil {
                        fmt.Println(err)
                        return nil, err
                }
                list = append(list, appname+"-"+space)
        }
        err = rows.Err()
        if err != nil {
                fmt.Println(err)
                return nil, err
        }
        db.Close()
        return list, nil
}

func getMetrics(_cluster string, _vhost string, appspace string) (e error) {
	client := http.Client{}
	var req *http.Request
	var err error
	if _cluster == "sandbox" {
		req, err = http.NewRequest("GET", sandboxapi+"/api/queues/"+_vhost, nil)
		req.SetBasicAuth(sandboxuser, sandboxpassword)
	}
	if _cluster == "live" {
		req, err = http.NewRequest("GET", liveapi+"/api/queues/"+_vhost, nil)
		req.SetBasicAuth(liveuser, livepassword)
	}
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	bodybytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var queues []RabbitMQQueue
	err = json.Unmarshal(bodybytes, &queues)
	if err != nil {
		fmt.Println("Unmarshal")
		fmt.Println(err)
		os.Exit(1)
	}

	for _, element := range queues {
		vhost := element.Vhost
		cluster := _cluster
		queuename := element.Name
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.DeliverGetDetails.Rate", element.MessageStats.DeliverGetDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.DeliverGet", element.MessageStats.DeliverGet))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.AckDetails.Rate", element.MessageStats.AckDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.Ack", element.MessageStats.Ack))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.RedeliverDetails.Rate", element.MessageStats.RedeliverDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.Redeliver", element.MessageStats.Redeliver))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.DeliverNoAckDetails.Rate", element.MessageStats.DeliverNoAckDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.DeliverNoAck", element.MessageStats.DeliverNoAck))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.DeliverDetails.Rate", element.MessageStats.DeliverDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.Deliver", element.MessageStats.Deliver))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.GetNoAckDetails.Rate", element.MessageStats.GetNoAckDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.GetNoAck", element.MessageStats.GetNoAck))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.GetDetails.Rate", element.MessageStats.GetDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.Get", element.MessageStats.Get))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.PublishDetails.Rate", element.MessageStats.PublishDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessageStats.Publish", element.MessageStats.Publish))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessagesDetails.Rate", element.MessagesDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "Messages", element.Messages))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessagesUnacknowledgedDetails.Rate", element.MessagesUnacknowledgedDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessagesUnacknowledged", element.MessagesUnacknowledged))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessagesReadyDetails.Rate", element.MessagesReadyDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "MessagesReady", element.MessagesReady))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "ReductionsDetails.Rate", element.ReductionsDetails.Rate))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "Reductions", element.Reductions))
		deliver(formatMetric(cluster, vhost, queuename, appspace, "Consumers", element.Consumers))
	}

	return nil

}

func getRabbitmqMetrics(appspaces []string) {

	uri := pitdb
	db, dberr := sql.Open("postgres", uri)
	if dberr != nil {
		fmt.Println(dberr)
		os.Exit(1)
	}
      for _, element := range appspaces {
	rows, err := db.Query("select bindname from appbindings where appname||'-'||space = '"+element+"' and bindtype='rabbitmq'")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer rows.Close()
        var bindname string
	for rows.Next() {
		err := rows.Scan(&bindname)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
         }
        cluster := getCluster(bindname)

        fmt.Printf("adding metrics for application owner %s on vhost %s on %s cluster\n", element, bindname, cluster)
        getMetrics(cluster, bindname, element)
     }

}

func getCluster(bindname string) (c string){

        uri := brokerdb
        db, dberr := sql.Open("postgres", uri)
        if dberr != nil {
                fmt.Println(dberr)
                os.Exit(1)
        }
        rows, err := db.Query("select cluster from provision where vhost ='"+bindname+"'")
        if err != nil {
                fmt.Println(err)
                os.Exit(1)
        }
        defer rows.Close()
        var cluster string
        for rows.Next() {
                err := rows.Scan(&cluster)
                if err != nil {
                        fmt.Println(err)
                        os.Exit(1)
                }
         }
        return cluster

}

func formatMetric(cluster string, vhost string, queue string, appspace string, metric string, value interface{}) (m string) {
	timestamp := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	put := fmt.Sprintf("put rabbitmq.queue.%v %v %v cluster=%v vhost=%v queue=%v app=%v\n", metric, timestamp, value, cluster, vhost, queue, appspace)
	return put
}

func deliver(put string) {
        fmt.Fprintf(tsdbconn, put)


}
