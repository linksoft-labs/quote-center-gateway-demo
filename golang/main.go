package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"go-quote-center-gateway-demo/pb"

	"google.golang.org/protobuf/proto"
)

func sendMsg(conn net.Conn, data []byte) error {
	length := len(data)
	buf := make([]byte, 4+len(data))
	// set header
	binary.BigEndian.PutUint32(buf, uint32(length))
	copy(buf[4:], data)
	_, err := conn.Write(buf)
	return err
}

func parse(wg *sync.WaitGroup, conn net.Conn) {
	defer wg.Done()

	var lenBuff [4]byte
	reader := bufio.NewReader(conn)
	for {
		clear(lenBuff[:])
		// the first 4 bytes for the message header
		n, err := io.ReadAtLeast(reader, lenBuff[:], 4)
		if err != nil {
			panic(err)
		}
		if n != 4 {
			panic("short read header")
		}
		length := binary.BigEndian.Uint32(lenBuff[:])
		buf := make([]byte, length)
		n, err = io.ReadAtLeast(reader, buf, int(length))
		if err != nil {
			panic(err)
		}
		if n != int(length) {
			fmt.Printf("length mismatch: %d != %d\n", n, length)
			continue
		}
		resp := &pb.Response{}
		err = proto.Unmarshal(buf[:n], resp)
		if err != nil {
			panic(err)
		}
		if resp.Response.GetType() != pb.ResponseType_ResponseType_SUCCESS {
			_, _ = fmt.Fprintln(os.Stderr, "error resp:", resp.Response.GetMsg())
			continue
		}
		fmt.Println("received response successfully", resp.ActionType.String())
		if resp.GetActionType() == pb.ActionType_ActionType_PUSHDATA {
			// handle data
			data := resp.GetSecurityData()
			fmt.Println("data", data)
			// processing fractional shares of US and USOTC stocks
			for _, d := range data {
				quote := d.GetQuote()
				if quote != nil {
					fmt.Printf("volume: %f", float64(quote.GetVolume())+quote.GetVolumeFloatPart())
				}
			}
		}
	}
}

func heartbeats(wg *sync.WaitGroup, conn net.Conn) {
	defer wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	heartBeatAct := pb.ActionType_ActionType_HEARTBEAT
	req := pb.Request{
		ActionType: &heartBeatAct,
	}
	reqBytes, err := proto.Marshal(&req)
	if err != nil {
		panic(err)
	}
	for range ticker.C {
		fmt.Println("sending heartbeats")
		err := sendMsg(conn, reqBytes)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <US|SSE|SZSE|HKEX|DCE|SHFE|FOREX|USOTC>")
		os.Exit(1)
	}
	addr, err := net.ResolveTCPAddr("tcp", os.Getenv("SERVER_ADDR"))
	if err != nil {
		panic(err)
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go parse(&wg, conn)

	// login
	loginAct := pb.ActionType_ActionType_LOGIN
	appid := os.Getenv("APP_ID")
	appSecret := os.Getenv("APP_SECRET")
	auth := pb.Auth{
		AppId:     &appid,
		AppSecret: &appSecret,
	}
	login := pb.Request{
		ActionType: &loginAct,
		Auth:       &auth,
		Security:   nil,
	}
	loginReply, err := proto.Marshal(&login)
	if err != nil {
		panic(err)
	}
	err = sendMsg(conn, loginReply)
	if err != nil {
		panic(err)
	}

	// waiting for login to finish
	time.Sleep(1 * time.Second)

	wg.Add(1)
	go heartbeats(&wg, conn)

	// subscribe for quote data
	subAct := pb.ActionType_ActionType_SUBSCRIBE
	market := pb.Market(0)
	source := pb.Source(0)
	symbol := "*"
	switch os.Args[1] {
	case "US":
		market = pb.Market_Market_US
		source = pb.Source_Source_NASDAQS
		symbol = "AAPL"
	case "USOTC":
		market = pb.Market_Market_USOTC
		source = pb.Source_Source_OTHER
		symbol = "DIDIY"
	case "SSE":
		market = pb.Market_Market_SSE
		source = pb.Source_Source_SSE
	case "HKEX":
		market = pb.Market_Market_HKEX
		source = pb.Source_Source_HKEX
	case "SZSE":
		market = pb.Market_Market_SZSE
		source = pb.Source_Source_SZSE
	case "DCE":
		market = pb.Market_Market_DCE
		source = pb.Source_Source_OTHER
	case "SHFE":
		market = pb.Market_Market_SHFE
		source = pb.Source_Source_OTHER
	case "FOREX":
		market = pb.Market_Market_FOREX
		source = pb.Source_Source_OTHER
		symbol = "JPYCNY,USDJPY"
	default:
		panic("unknown market type")
	}

	subType := pb.SubType_SubType_QUOTE
	securities := make([]*pb.Security, 0)
	// flag := pb.Flag_Flag_GRAY 港交所暗盘
	flag := pb.Flag_Flag_REALTIME
	for sec := range strings.SplitSeq(symbol, ",") {
		securities = append(securities, &pb.Security{
			Market:  &market,
			Symbol:  &sec,
			SubType: &subType,
			Source:  &source,
			Flag:    &flag,
		})
	}
	sub := pb.Request{
		ActionType: &subAct,
		Auth:       nil,
		Security:   securities,
	}
	subRequest, err := proto.Marshal(&sub)
	if err != nil {
		panic(err)
	}
	err = sendMsg(conn, subRequest)
	if err != nil {
		panic(err)
	}

	wg.Wait()
}
