//go:build ignore

package queryopenapi

// ReportFieldValueRefsExtractor is an interface for custom field value types
// to provide their referenced URNs for dependency ordering.
// If a custom type implements this interface, GetReferencedUrns will use it.
type ReportFieldValueRefsExtractor interface {
	GetReferencedUrns() []string
}

// GetReferencedUrns returns all URNs that this field value references.
// For unknown discriminator types, it checks if the type implements
// ReportFieldValueRefsExtractor, otherwise returns nil.
func (t ApiReportFieldValue) GetReferencedUrns() []string {
	discriminator, err := t.Discriminator()
	if err != nil {
		return nil
	}

	switch discriminator {
	case "ref":
		v, err := t.AsApiRefReportFieldValue()
		if err != nil {
			return nil
		}
		return []string{v.Urn}

	case "constant":
		return nil

	case "condition":
		v, err := t.AsApiConditionReportFieldValue()
		if err != nil {
			return nil
		}
		return appendUrns(v.Operand1.GetReferencedUrns(), v.Operand2.GetReferencedUrns())

	case "logicalExpression":
		v, err := t.AsApiLogicalExpressionReportFieldValue()
		if err != nil {
			return nil
		}
		return appendUrns(v.Operand1.GetReferencedUrns(), v.Operand2.GetReferencedUrns())

	case "selector":
		v, err := t.AsApiSelectorReportFieldValue()
		if err != nil {
			return nil
		}
		return appendUrns(
			v.SelectorBooleanField.GetReferencedUrns(),
			v.TrueField.GetReferencedUrns(),
			v.FalseField.GetReferencedUrns(),
		)

	case "nvl":
		v, err := t.AsApiNvlReportFieldValue()
		if err != nil {
			return nil
		}
		return appendUrns(v.Source.GetReferencedUrns(), v.AltField.GetReferencedUrns())

	case "cast":
		v, err := t.AsApiCastReportFieldValue()
		if err != nil {
			return nil
		}
		return v.Source.GetReferencedUrns()

	case "numericExpression":
		v, err := t.AsApiNumericExpressionReportFieldValue()
		if err != nil {
			return nil
		}
		return appendUrns(v.Op1.GetReferencedUrns(), v.Op2.GetReferencedUrns())

	case "unaryNumericOperator":
		v, err := t.AsApiUnaryNumericOperatorReportFieldValue()
		if err != nil {
			return nil
		}
		return v.Operand.GetReferencedUrns()

	case "reduce":
		v, err := t.AsApiReduceReportFieldValue()
		if err != nil {
			return nil
		}
		// reduce.FieldUrns are direct URN references
		return v.FieldUrns

	default:
		// Try to use the interface for custom types
		val, err := t.ValueByDiscriminator()
		if err != nil {
			return nil
		}
		if extractor, ok := val.(ReportFieldValueRefsExtractor); ok {
			return extractor.GetReferencedUrns()
		}
		return nil
	}
}

// appendUrns combines multiple URN slices into one
func appendUrns(slices ...[]string) []string {
	var result []string
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}
