package dremio

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/knowledgesystems/smile-dremio-gateway/internal/arrowflight"
	"github.com/knowledgesystems/smile-dremio-gateway/internal/smile"
	"strings"
)

type DremioArgs struct {
	Host         string
	Username     string
	Password     string
	ObjectStore  string
	RequestTable string
	SampleTable  string
}

type DremioRepository struct {
	args DremioArgs
}

func NewDremioRepos(args DremioArgs) (*DremioRepository, error) {
	if args.Host == "" {
		return nil, errors.New("host must not be empty")
	}
	if args.Username == "" {
		return nil, errors.New("username must not be empty")
	}
	if args.Password == "" {
		return nil, errors.New("password must not be empty")
	}
	if args.ObjectStore == "" {
		return nil, errors.New("objectstore must not be empty")
	}
	if args.RequestTable == "" {
		return nil, errors.New("requesttable must not be empty")
	}
	if args.SampleTable == "" {
		return nil, errors.New("sampletable must not be empty")
	}
	return &DremioRepository{args: args}, nil
}

func (r *DremioRepository) AddRequest(ctx context.Context, sr smile.Request) error {
	af, err := arrowflight.NewArrowFlight(r.args.Host, r.args.Username, r.args.Password)
	if err != nil {
		return err
	}
	defer af.FC.Close()

	// lets check for existing request, if exists remove it and its samples
	existingRequests, err := r.getRequests(af, sr)
	if err != nil {
		return err
	}
	if existingRequests != nil {
		if len(existingRequests) > 0 {
			err = r.removeRequest(af, existingRequests[0])
			if err != nil {
				return err
			}
			err = r.removeSamples(af, existingRequests[0])
			if err != nil {
				return err
			}
		}
	}

	// lets save samples first, because we want to remove them from request before saving request
	// its also more likely that we will encounter an error here than when saving a request because
	// 1 request -> 1 or more samples
	err = r.insertSamples(af, sr)
	if err != nil {
		// remove any inserted samples before failure where IGO_REQUEST_ID == sr.IgoRequestID
		r.removeSamples(af, sr)
		return err
	}

	err = r.insertRequest(af, sr)
	if err != nil {
		// remove inserted samples where IGO_REQUEST_ID == sr.IgoRequestID
		r.removeSamples(af, sr)
		return err
	}

	return nil
}

func (r *DremioRepository) getRequests(af *arrowflight.ArrowFlight, sr smile.Request) ([]smile.Request, error) {
	query := fmt.Sprintf("select * from %s.%s where IGO_REQUEST_ID = '%s'", r.args.ObjectStore, r.args.RequestTable, sr.IgoRequestID)
	rdr, err := af.Query(query)
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	var requests []smile.Request
	for rdr.Next() {
		rec := rdr.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			var r smile.Request
			for j := 0; j < int(rec.NumCols()); j++ {
				switch rec.ColumnName(j) {
				case "IGO_REQUEST_ID":
					continue
				case "REQUEST_JSON":
					err = json.Unmarshal([]byte(rec.Column(j).(*array.String).Value(i)), &r)
					if err != nil {
						return nil, err
					} else {
						requests = append(requests, r)
					}
				}
			}
		}
		rec.Release()
	}
	return requests, nil
}

func (r *DremioRepository) removeRequest(af *arrowflight.ArrowFlight, sr smile.Request) error {
	query := fmt.Sprintf("delete from %s.%s where IGO_REQUEST_ID = '%s'", r.args.ObjectStore, r.args.RequestTable, sr.IgoRequestID)
	_, err := af.Query(query)
	if err != nil {
		return err
	}
	return nil
}

func (r *DremioRepository) insertSamples(af *arrowflight.ArrowFlight, sr smile.Request) error {

	var b strings.Builder
	fmt.Fprintf(&b, "insert into %s.%s values ", r.args.ObjectStore, r.args.SampleTable)
	for _, s := range sr.Samples {
		sJson, err := json.Marshal(s)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "('%s', '%s', '%s', '%s'),", sr.IgoRequestID, s.SampleName, s.CmoSampleName, string(sJson))
	}
	query := b.String()
	query = strings.TrimRight(query, ",")
	_, err := af.Query(query)
	if err != nil {
		return err
	}
	return nil
}

func (r *DremioRepository) insertRequest(af *arrowflight.ArrowFlight, sr smile.Request) error {
	// clobber samples in sr,Samples[] before saving because they just got stored in the samples table
	sr.Samples = sr.Samples[:0]
	rJson, err := json.Marshal(sr)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("insert into %s.%s values ('%s', '%s')", r.args.ObjectStore, r.args.RequestTable, sr.IgoRequestID, string(rJson))
	_, err = af.Query(query)
	if err != nil {
		return err
	}
	return nil
}

func (r *DremioRepository) removeSamples(af *arrowflight.ArrowFlight, sr smile.Request) error {
	query := fmt.Sprintf("delete from %s.%s where IGO_REQUEST_ID = '%s'", r.args.ObjectStore, r.args.SampleTable, sr.IgoRequestID)
	_, err := af.Query(query)
	if err != nil {
		return err
	}
	return nil
}

func (r *DremioRepository) UpdateRequest(ctx context.Context, sr []smile.Request) error {
	if len(sr) < 2 {
		return fmt.Errorf("request metadata array contains less than two entries: %d", len(sr))
	}
	af, err := arrowflight.NewArrowFlight(r.args.Host, r.args.Username, r.args.Password)
	if err != nil {
		return err
	}
	defer af.FC.Close()

	err = r.updateRequest(af, sr)
	if err != nil {
		return err
	}

	return nil
}

func (r *DremioRepository) updateRequest(af *arrowflight.ArrowFlight, sr []smile.Request) error {
	rJson, err := json.Marshal(sr[0])
	if err != nil {
		return err
	}
	query := fmt.Sprintf("update %s.%s set IGO_REQUEST_ID = '%s', REQUEST_JSON = '%s' where IGO_REQUEST_ID = '%s'", r.args.ObjectStore, r.args.RequestTable, sr[0].IgoRequestID, string(rJson), sr[1].IgoRequestID)
	rdr, err := af.Query(query)
	if err != nil {
		return err
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		defer rec.Release()
		for i := 0; i < int(rec.NumRows()); i++ {
			for j := 0; j < int(rec.NumCols()); j++ {
				switch rec.ColumnName(j) {
				case "Records":
					result := rec.Column(j).(*array.Int64).Value(i)
					if result == 0 {
						return fmt.Errorf("Update failed, most likely cause is IGO Request Id in where close cannot be found: %s", sr[1].IgoRequestID)
					}
				}
			}
		}
	}

	return nil
}

func (r *DremioRepository) UpdateSample(ctx context.Context, s []smile.Sample) error {
	if len(s) < 2 {
		return fmt.Errorf("sample metadata array contains less than two entries: %d", len(s))
	}
	af, err := arrowflight.NewArrowFlight(r.args.Host, r.args.Username, r.args.Password)
	if err != nil {
		return err
	}
	defer af.FC.Close()

	err = r.updateSample(af, s)
	if err != nil {
		return err
	}

	return nil
}

func (r *DremioRepository) updateSample(af *arrowflight.ArrowFlight, s []smile.Sample) error {
	sJson, err := json.Marshal(s[0])
	if err != nil {
		return err
	}
	// []smile.Sample is an ordered list of metadata in decending order:
	// s[0] is most recent, s[1] is what is currently in dremio table
	query := fmt.Sprintf("update %s.%s set IGO_REQUEST_ID = '%s', IGO_SAMPLE_NAME = '%s', CMO_SAMPLE_NAME = '%s', SAMPLE_JSON = '%s' where IGO_REQUEST_ID = '%s' and IGO_SAMPLE_NAME = '%s' and CMO_SAMPLE_NAME = '%s'", r.args.ObjectStore, r.args.SampleTable, s[0].AdditionalProperties.IgoRequestID, s[0].SampleName, s[0].CmoSampleName, string(sJson), s[1].AdditionalProperties.IgoRequestID, s[1].SampleName, s[1].CmoSampleName)
	rdr, err := af.Query(query)
	if err != nil {
		return err
	}
	defer rdr.Release()
	for rdr.Next() {
		rec := rdr.Record()
		defer rec.Release()
		for i := 0; i < int(rec.NumRows()); i++ {
			for j := 0; j < int(rec.NumCols()); j++ {
				switch rec.ColumnName(j) {
				case "Records":
					result := rec.Column(j).(*array.Int64).Value(i)
					if result == 0 {
						return fmt.Errorf("Update failed, most likely cause is IGO Request Id or IGO_SAMPLE_NAME or CMO_SAMPLE_NAME in where close cannot be found: %s %s %s", s[1].AdditionalProperties.IgoRequestID, s[1].SampleName, s[1].CmoSampleName)
					}
				}
			}
		}
	}

	return nil
}
