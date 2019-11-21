package api

type ResponseEntity struct {
	Code    int64       `json:"code,omitempty"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type ListResponseEntity struct {
	Code    int64       `json:"code,omitempty"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Total   int64       `json:"total,omitempty"`
	Page    int64       `json:"page,omitempty"`
	Limit   int64       `json:"limit,omitempty"`
}
