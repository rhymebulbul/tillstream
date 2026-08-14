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

type Generator struct {
	rng   *rand.Rand
	faker *gofakeit.Faker
}

func NewGenerator(seed int64) *Generator {
	return &Generator{
		rng:   rand.New(rand.NewSource(seed)),
		faker: gofakeit.New(seed),
	}
}

func (g *Generator) GetRandomTenantID() string {
	if g.rng.Intn(100) < 80 {
		return tenants[g.rng.Intn(2)]
	}
	return tenants[2+g.rng.Intn(3)]
}

func (g *Generator) GenerateOrderFlow() (Order, Payment) {
	tenantID := g.GetRandomTenantID()
	orderID := g.faker.UUID()
	storeID := fmt.Sprintf("%s_STORE_%d", tenantID, g.rng.Intn(5)+1)
	totalPrice := g.faker.Price(5.0, 500.0)

	now := time.Now().UTC()

	order := Order{
		OrderID:       orderID,
		TenantID:      tenantID,
		StoreID:       storeID,
		CustomerID:    g.faker.UUID(),
		LoyaltyPoints: g.rng.Intn(100),
		TotalPrice:    totalPrice,
		CreatedAt:     now.Format(time.RFC3339),
	}

	payment := Payment{
		PaymentID:     g.faker.UUID(),
		OrderID:       orderID,
		TenantID:      tenantID,
		Amount:        totalPrice,
		PaymentMethod: g.faker.RandomString([]string{"CREDIT_CARD", "DEBIT_CARD", "CASH", "DIGITAL_WALLET"}),
		Status:        "COMPLETED",
		CreatedAt:     now.Add(time.Second * time.Duration(g.rng.Intn(5))).Format(time.RFC3339),
	}

	return order, payment
}

// Keep the global one for backward compatibility if needed by other files
var defaultGen = NewGenerator(time.Now().UnixNano())

func GenerateOrderFlow() (Order, Payment) {
	return defaultGen.GenerateOrderFlow()
}
