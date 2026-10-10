package pipeline_test

import (
	"context"
	"fmt"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/pipeline"
)

type Account struct {
	ID    string `json:"id"`
	Limit int    `json:"limit"`
}

// An object saved by one step is read by the next, with the JSON helpers.
func ExampleSetJSON() {
	p, err := pipeline.New("checkout", "v1", []pipeline.Step{
		{Name: "fetch-user", Run: func(ctx context.Context, x *pipeline.Exec) error {
			return pipeline.SetJSON(x, "account", Account{ID: "a-1", Limit: 50})
		}},
		{Name: "charge", Run: func(ctx context.Context, x *pipeline.Exec) error {
			account, err := pipeline.RequireJSON[Account](x, "account")
			if err != nil {
				return err // permanent: the data is missing or unreadable
			}
			fmt.Println("charging", account.ID, "up to", account.Limit)
			return nil
		}},
	})
	if err != nil {
		return
	}
	res, _ := p.Execute(context.Background(), dlq.NewMemoryStore(dlq.MemoryOptions{}), pipeline.Input{ID: "m-1"})
	fmt.Println(res == pipeline.Done)
	// Output:
	// charging a-1 up to 50
	// true
}
