package smile

import (
	"encoding/json"
	"errors"
	nm "github.com/knowledgesystems/nats-messaging-go"
	"log"
	"strconv"
)

type SmileArgs struct {
	URL                 string
	CertPath            string
	KeyPath             string
	Consumer            string
	Password            string
	Subject             string
	NewRequestFilter    string
	UpdateRequestFilter string
	UpdateSampleFilter  string
}

type SmileAdaptor struct {
	SmileArgs SmileArgs
	Messaging *nm.Messaging
}

type RequestAdaptor struct {
	Requests []Request
	Msg      *nm.Msg
}

type SampleAdaptor struct {
	Samples []Sample
	Msg     *nm.Msg
}

func NewSmileAdaptor(sa SmileArgs) (*SmileAdaptor, error) {
	if sa.URL == "" {
		return nil, errors.New("url cannot be nil")
	}

	m, err := nm.NewSecureMessaging(sa.URL, sa.CertPath, sa.KeyPath, sa.Consumer, sa.Password)
	if err != nil {
		return nil, errors.New("cannot create a messaging connection")
	}

	return &SmileAdaptor{SmileArgs: sa, Messaging: m}, nil
}

func (s SmileAdaptor) SubscribeSmileConsumer(newRequestCh chan RequestAdaptor, upRequestCh chan RequestAdaptor, upSampleCh chan SampleAdaptor) error {
	err := s.Messaging.Subscribe(s.SmileArgs.Consumer, s.SmileArgs.Subject, func(m *nm.Msg) {
		switch {
		case m.Subject == s.SmileArgs.NewRequestFilter:
			var r Request
			su, err := strconv.Unquote(string(m.Data))
			if err != nil {
				log.Println("Error unmarshaling Request: ", err)
			} else {
				err = json.Unmarshal([]byte(su), &r)
				if err != nil {
					log.Println("Error unmarshaling Request: ", err)
				} else {
					reqs := []Request{r}
					ra := RequestAdaptor{reqs, m}
					newRequestCh <- ra
				}
			}
			break
		case m.Subject == s.SmileArgs.UpdateRequestFilter:
			var r []Request
			su, err := strconv.Unquote(string(m.Data))
			if err != nil {
				log.Println("Error unmarshaling Request: ", err)
			} else {
				err := json.Unmarshal([]byte(su), &r)
				if err != nil {
					log.Println("Error unmarshaling Request: ", err)
				} else {
					ra := RequestAdaptor{r, m}
					upRequestCh <- ra
				}
			}
			break
		case m.Subject == s.SmileArgs.UpdateSampleFilter:
			var s []Sample
			su, err := strconv.Unquote(string(m.Data))
			if err != nil {
				log.Println("Error unmarshaling Request: ", err)
			} else {
				err := json.Unmarshal([]byte(su), &s)
				if err != nil {
					log.Println("Error unmarshaling Sample: ", err)
				} else {
					sa := SampleAdaptor{s, m}
					upSampleCh <- sa
				}
			}
			break
		default:
			// not interested in message, Ack it so we don't get it again
			m.ProviderMsg.Ack()
		}
	})
	return err
}

func (s SmileAdaptor) AckRequest(ra RequestAdaptor) {
	ra.Msg.ProviderMsg.Ack()
}

func (s SmileAdaptor) AckSample(sa SampleAdaptor) {
	sa.Msg.ProviderMsg.Ack()
}

func (s SmileAdaptor) Shutdown() {
	s.Messaging.Shutdown()
}
