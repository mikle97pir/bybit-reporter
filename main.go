package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
)

type BybitClient struct {
	conn         *websocket.Conn
	httpClient   *http.Client
	baseWsUrl    url.URL
	baseHttpUrl  url.URL
	pingInterval time.Duration
	retryDelay   time.Duration
	topics       []string
}

type BybitTicker struct {
	Symbol        string  `json:"symbol"`
	Bid1Price     float64 `json:"bid1Price,string"`
	Bid1Size      float64 `json:"bid1Size,string"`
	Ask1Price     float64 `json:"ask1Price,string"`
	Ask1Size      float64 `json:"ask1Size,string"`
	LastPrice     float64 `json:"lastPrice,string"`
	PrevPrice24h  float64 `json:"prevPrice24h,string"`
	Price24hPcnt  float64 `json:"price24hPcnt,string"`
	HighPrice24h  float64 `json:"highPrice24h,string"`
	LowPrice24h   float64 `json:"lowPrice24h,string"`
	Turnover24h   float64 `json:"turnover24h,string"`
	Volume24h     float64 `json:"volume24h,string"`
	UsdIndexPrice float64 `json:"usdIndexPrice,string"`
}

type BybitTickerResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string        `json:"category"`
		List     []BybitTicker `json:"list"`
	} `json:"result"`
	RetExtInfo interface{} `json:"retExtInfo"`
	Time       uint64      `json:"time"`
}

type BybitSubscribeMessage struct {
	ReqID string   `json:"req_id,omitempty"`
	Op    string   `json:"op"`
	Args  []string `json:"args"`
}

type BybitTradeMessage struct {
	Topic string       `json:"topic"`
	Ts    int64        `json:"ts"`
	Type  string       `json:"type"`
	Data  []BybitTrade `json:"data"`
}

type BybitTrade struct {
	Id      int64   `json:"i,string"`
	Ts      int64   `json:"T"`
	Price   float64 `json:"p,string"`
	Size    float64 `json:"v,string"`
	Side    string  `json:"S"`
	Symbol  string  `json:"s"`
	IsBlock bool    `json:"BT"`
}

func NewBybitClient(baseWsUrl string, baseHttpUrl string, pingInterval time.Duration, retryDelay time.Duration) (*BybitClient, error) {
	wsUrl, err := url.Parse(baseWsUrl)
	if err != nil {
		return nil, err
	}

	httpUrl, err := url.Parse(baseHttpUrl)
	if err != nil {
		return nil, err
	}

	return &BybitClient{
		httpClient:   &http.Client{},
		baseWsUrl:    *wsUrl,
		baseHttpUrl:  *httpUrl,
		pingInterval: pingInterval,
		retryDelay:   retryDelay,
	}, nil
}

func (c *BybitClient) GetTickers(ctx context.Context) (*BybitTickerResponse, error) {
	endpointUrl := c.baseHttpUrl.JoinPath("v5", "market", "tickers")
	params := url.Values{}
	params.Add("category", "spot")
	endpointUrl.RawQuery = params.Encode()

	resp, err := http.Get(endpointUrl.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response BybitTickerResponse

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *BybitClient) TickersToInstruments(response *BybitTickerResponse) []string {
	var instruments []string
	for _, ticker := range response.Result.List {
		instruments = append(instruments, ticker.Symbol)
	}
	return instruments
}

func (c *BybitClient) InstrumentsToTopics(instruments []string) []string {
	var topics []string
	for _, instrument := range instruments {
		topics = append(topics, "publicTrade."+instrument)
	}
	return topics
}

func (c *BybitClient) dial(ctx context.Context) error {
	endpointUrl := c.baseWsUrl.JoinPath("v5", "public", "spot")
	var err error
	c.conn, _, err = websocket.DefaultDialer.DialContext(ctx, endpointUrl.String(), nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *BybitClient) listen(ctx context.Context, errors chan error, messages chan []byte) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("listening canceled")
			return
		default:
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				select {
				case errors <- err:
					slog.Debug("sent listening error")
				default:
					slog.Debug("cannot send listening error, channel is full")
				}
			}
			slog.Debug("received message")
			messages <- message
		}
	}
}

func (c *BybitClient) ping(ctx context.Context, errors chan error) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("canceled ping")
			return
		default:
			err := c.conn.WriteMessage(websocket.TextMessage, []byte("{\"op\": \"ping\"}"))
			if err != nil {
				select {
				case errors <- err:
					slog.Debug("send ping error")
				default:
					slog.Debug("cannot send ping error, channel is full")
				}
			}
			slog.Info("sent ping")
			time.Sleep(c.pingInterval)
		}
	}
}

func (c *BybitClient) subscribe(topics []string) error {
	message := BybitSubscribeMessage{
		Op:   "subscribe",
		Args: topics,
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = c.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return err
	}

	return nil
}

func (c *BybitClient) subscribePages(topics []string) error {
	i := 0
	for i < len(topics) {
		j := min(i+10, len(topics))
		err := c.subscribe(topics[i:j])
		if err != nil {
			return err
		}
		slog.Debug("subscribed", slog.Int("i", i), slog.Int("j", j))
		i += 10
	}
	return nil
}

func (c *BybitClient) startListening(ctx context.Context, errors chan error, messages chan []byte) (context.CancelFunc, error) {

	err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	slog.Debug("websocket connection established")

	err = c.subscribePages(c.topics)
	if err != nil {
		return nil, err
	}
	slog.Debug("subscribed")

	ctxWs, cancelWs := context.WithCancel(ctx)

	go c.ping(ctxWs, errors)
	go c.listen(ctxWs, errors, messages)

	return cancelWs, nil

}

func (c *BybitClient) handleMessage(ctx context.Context, message []byte, handledMessages chan BybitTradeMessage) {
	messageStr := string(message)
	if strings.Contains(messageStr, "topic") {
		var parsedMessage BybitTradeMessage
		err := json.Unmarshal(message, &parsedMessage)
		if err != nil {
			slog.Error("Cannot parse message", slog.String("body", messageStr))
		}
		slog.Debug("message", slog.Any("body", parsedMessage))
		handledMessages <- parsedMessage
	}
}

func openDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "trades.db")
	if err != nil {
		return nil, err
	}
	createTable := `
	CREATE TABLE IF NOT EXISTS BybitTrade (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		written_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
		topic TEXT NOT NULL,
		message_ts INTEGER NOT NULL,
		message_type TEXT NOT NULL,
		trade_id INTEGER NOT NULL,
		trade_ts INTEGER NOT NULL,
		price REAL NOT NULL,
		size REAL NOT NULL,
		side TEXT NOT NULL,
		symbol TEXT NOT NULL,
		is_block BOOLEAN NOT NULL
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		db.Close()
		return nil, err
	}
	_, err = db.Exec("BEGIN TRANSACTION")
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func closeDb(db *sql.DB) {
	_, err := db.Exec("END TRANSACTION")
	if err != nil {
		slog.Error("cannot close db", slog.Any("error", err))
	}
	err = db.Close()
	if err != nil {
		slog.Error("cannot close db", slog.Any("error", err))
	}
}

func writeMessagesToDb(ctx context.Context, db *sql.DB, handledMessages chan BybitTradeMessage) {
	insert := `
	INSERT INTO BybitTrade (topic, message_ts, message_type, trade_id, trade_ts, price, size, side, symbol, is_block)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	for {
		select {
		case <-ctx.Done():
			slog.Debug("writing to db canceled")
			return
		default:
			message := <-handledMessages
			for _, trade := range message.Data {
				_, err := db.Exec(
					insert,
					message.Topic,
					message.Ts,
					message.Type,
					trade.Id,
					trade.Ts,
					trade.Price,
					trade.Size,
					trade.Side,
					trade.Symbol,
					trade.IsBlock,
				)
				if err != nil {
					slog.Error("cannot write to db", slog.Any("body", message))
				}
				slog.Debug("inserted trade")
			}
		}
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	ctx := context.Background()

	db, err := openDb()
	if err != nil {
		slog.Error("cannot init db", slog.Any("error", err))
		os.Exit(1)
	}
	defer closeDb(db)

	c, err := NewBybitClient(
		"wss://stream.bybit.com",
		"https://api.bybit.com",
		20*time.Second,
		20*time.Second,
	)
	if err != nil {
		slog.Error("cannot create bybit client", slog.Any("error", err))
		os.Exit(1)
	}

	tickers, err := c.GetTickers(ctx)
	if err != nil {
		slog.Error("cannot get tickers", slog.Any("error", err))
	}

	instruments := c.TickersToInstruments(tickers)
	c.topics = c.InstrumentsToTopics(instruments)
	slog.Info("topics", slog.Any("body", c.topics))

	messages := make(chan []byte)
	errors := make(chan error)

	cancelWs, err := c.startListening(ctx, errors, messages)
	if err != nil {
		slog.Error("cannot start ws", slog.Any("error", err))
	}

	handledMessages := make(chan BybitTradeMessage)
	go writeMessagesToDb(ctx, db, handledMessages)

	for {
		select {
		case message := <-messages:
			go c.handleMessage(ctx, message, handledMessages)
		case err := <-errors:
			slog.Error("error", slog.Any("error", err))
			cancelWs()

			slog.Info("waiting before redialing")
			time.Sleep(c.retryDelay)

			slog.Info("redialing")
			cancelWs, err = c.startListening(ctx, errors, messages)
			if err != nil {
				slog.Error("cannot start ws", slog.Any("error", err))
			}
		}
	}
}
