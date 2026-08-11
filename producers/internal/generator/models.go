package generator

type Order struct {
	OrderID    string  `avro:"order_id"`
	TenantID   string  `avro:"tenant_id"`
	StoreID    string  `avro:"store_id"`
	CustomerID string  `avro:"customer_id"`
	TotalItems int     `avro:"total_items"`
	TotalPrice float64 `avro:"total_price"`
	CreatedAt  string  `avro:"created_at"`
}

type Payment struct {
	PaymentID     string  `avro:"payment_id"`
	OrderID       string  `avro:"order_id"`
	TenantID      string  `avro:"tenant_id"`
	Amount        float64 `avro:"amount"`
	PaymentMethod string  `avro:"payment_method"`
	Status        string  `avro:"status"`
	CreatedAt     string  `avro:"created_at"`
}
