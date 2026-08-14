package generator

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
)

var tenants = []string{
	"TENANT_FLAGSHIP_1", "TENANT_FLAGSHIP_2",
	"TENANT_LOCAL_1", "TENANT_LOCAL_2", "TENANT_LOCAL_3",
}

func GetRandomTenantID() string {
	if rand.Intn(100) < 80 {
		return tenants[rand.Intn(2)]
	}
	return tenants[2+rand.Intn(3)]
}

func GenerateOrderFlow() (Order, Payment) {
	tenantID := GetRandomTenantID()
	orderID := gofakeit.UUID()
	storeID := fmt.Sprintf("%s_STORE_%d", tenantID, rand.Intn(5)+1)
	totalPrice := gofakeit.Price(5.0, 500.0)

	order := Order{
		OrderID:    orderID,
		TenantID:   tenantID,
		StoreID:       storeID,
		CustomerID:    gofakeit.UUID(),
		LoyaltyPoints: rand.Intn(100),
		TotalPrice:    totalPrice,
		CreatedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	payment := Payment{
		PaymentID:     gofakeit.UUID(),
		OrderID:       orderID,
		TenantID:      tenantID,
		Amount:        totalPrice,
		PaymentMethod: gofakeit.RandomString([]string{"CREDIT_CARD", "DEBIT_CARD", "CASH", "DIGITAL_WALLET"}),
		Status:        "COMPLETED",
		CreatedAt:     time.Now().UTC().Add(time.Second * time.Duration(rand.Intn(5))).Format(time.RFC3339),
	}

	return order, payment
}
