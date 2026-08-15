package generator

import (
	"testing"
)

func TestNewGenerator(t *testing.T) {
	g := NewGenerator(42)
	if g == nil {
		t.Fatal("Expected non-nil generator")
	}
}

func TestGenerateOrderFlow(t *testing.T) {
	g := NewGenerator(42)
	order, payment := g.GenerateOrderFlow()

	if order.OrderID == "" {
		t.Error("Expected non-empty OrderID")
	}
	if payment.PaymentID == "" {
		t.Error("Expected non-empty PaymentID")
	}
	if order.OrderID != payment.OrderID {
		t.Errorf("Expected OrderID %s to match Payment OrderID %s", order.OrderID, payment.OrderID)
	}
	if order.TenantID != payment.TenantID {
		t.Errorf("Expected TenantID %s to match Payment TenantID %s", order.TenantID, payment.TenantID)
	}
	if order.TotalPrice != payment.Amount {
		t.Errorf("Expected TotalPrice %f to match Payment Amount %f", order.TotalPrice, payment.Amount)
	}
}
