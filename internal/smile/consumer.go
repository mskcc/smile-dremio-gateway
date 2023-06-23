package smile

import (
	"context"
	"errors"
	"log"
	"sync"
)

const (
	requestBufSize = 1
	sampleBufSize  = 1
)

type SmileSubscriber interface {
	SubscribeSmileConsumer(newRequestCh chan RequestAdaptor, upRequestCh chan RequestAdaptor, upSampleCh chan SampleAdaptor) error
	AckRequest(ra RequestAdaptor)
	AckSample(sa SampleAdaptor)
	Shutdown()
}

type Repository interface {
	AddRequest(context.Context, Request) error
	UpdateRequest(context.Context, []Request) error
	UpdateSample(context.Context, []Sample) error
}

type Service struct {
	smile SmileSubscriber
	repo  Repository
}

func NewService(smile SmileSubscriber, repo Repository) (*Service, error) {
	if smile == nil {
		return nil, errors.New("SmileSubscriber must not be nil")
	}
	if repo == nil {
		return nil, errors.New("Repository must not be nil")
	}
	return &Service{smile: smile, repo: repo}, nil
}

func (svc *Service) Run(ctx context.Context) error {

	log.Println("Starting up SMILE consumer...")
	newRequestCh := make(chan RequestAdaptor, requestBufSize)
	updateRequestCh := make(chan RequestAdaptor, requestBufSize)
	updateSampleCh := make(chan SampleAdaptor, sampleBufSize)
	err := svc.smile.SubscribeSmileConsumer(newRequestCh, updateRequestCh, updateSampleCh)
	if err != nil {
		return err
	}
	log.Println("SMILE consumer running...")

	var nrwg sync.WaitGroup
	var urwg sync.WaitGroup
	var uswg sync.WaitGroup
	for {
		select {
		case ra := <-newRequestCh:
			nrwg.Add(1)
			log.Printf("Processing add request: %s\n", ra.Requests[0].IgoRequestID)
			go func() {
				defer nrwg.Done()
				err := svc.repo.AddRequest(ctx, ra.Requests[0])
				if err != nil {
					log.Println("Error adding request: ", err)
				}
				// if we don't ack, we will keep getting message
				svc.smile.AckRequest(ra)
			}()
			log.Println("Processing add request complete")
		case ra := <-updateRequestCh:
			urwg.Add(1)
			log.Printf("Processing update request: %s\n", ra.Requests[0].IgoRequestID)
			go func() {
				defer urwg.Done()
				err := svc.repo.UpdateRequest(ctx, ra.Requests)
				if err != nil {
					log.Println("Error updating request: ", err)
				}
				// if we don't ack, we will keep getting message
				svc.smile.AckRequest(ra)
			}()
			log.Println("Processing update request complete")
		case sa := <-updateSampleCh:
			uswg.Add(1)
			log.Printf("Processing update sample: %s\n", sa.Samples[0].CmoSampleName)
			go func() {
				defer uswg.Done()
				err := svc.repo.UpdateSample(ctx, sa.Samples)
				if err != nil {
					log.Println("Error updating sample: ", err)
				}
				// if we don't ack, we will keep getting message
				svc.smile.AckSample(sa)
			}()
			log.Println("Processing update sample complete")
		case <-ctx.Done():
			log.Println("Context canceled, returning...")
			// tbd: check for messages being processed
			nrwg.Wait()
			urwg.Wait()
			uswg.Wait()
			svc.smile.Shutdown()
			return nil
		}
	}
}
