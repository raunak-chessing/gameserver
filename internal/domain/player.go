package domain

import "time"

type Player struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Rating           int       `json:"rating"`
	
	RatingBullet     int       `json:"ratingBullet"`
	RDBullet         float64   `json:"rdBullet"`
	LastActiveBullet time.Time `json:"lastActiveBullet"`

	RatingBlitz      int       `json:"ratingBlitz"`
	RDBlitz          float64   `json:"rdBlitz"`
	LastActiveBlitz  time.Time `json:"lastActiveBlitz"`

	RatingRapid      int       `json:"ratingRapid"`
	RDRapid          float64   `json:"rdRapid"`
	LastActiveRapid  time.Time `json:"lastActiveRapid"`

	RatingDaily      int       `json:"ratingDaily"`
	RDDaily          float64   `json:"rdDaily"`
	LastActiveDaily  time.Time `json:"lastActiveDaily"`
}
