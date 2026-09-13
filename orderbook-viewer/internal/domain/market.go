package domain

// Market and Exchange are display metadata resolved from postgres.
// pair_id / exchange_id are the identity carried by the Flink output;
// these fields are display-only.

type Market struct {
	ID    int    `json:"id"`
	Base  string `json:"base"`
	Quote string `json:"quote"`
}

type Exchange struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Label string `json:"label"`
}
