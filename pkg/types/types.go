// Package types contains shared types for amq
package types

// Response represents a generic API response
type Response struct {
    Success bool        `json:"success"`
    Message string      `json:"message,omitempty"`
    Data    interface{} `json:"data,omitempty"`
}
