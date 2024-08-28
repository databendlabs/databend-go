package godatabend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type typeparserTestCase struct {
	desc   string
	input  string
	output *TypeDesc
	fail   bool
}

func TestParseTypeDesc(t *testing.T) {
	testCases := []*typeparserTestCase{
		{
			desc:  "plain type",
			input: "String",
			output: &TypeDesc{
				Name:     "String",
				Nullable: false,
				Args:     []*TypeDesc{},
			},
		},
		{
			desc:  "decimal type",
			input: "Decimal(42, 42)",
			output: &TypeDesc{
				Name:     "Decimal",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "42",
						Nullable: false,
						Args:     []*TypeDesc{},
					},
					{
						Name:     "42",
						Nullable: false,
						Args:     []*TypeDesc{},
					},
				},
			},
		},
		{
			desc:  "nullable type",
			input: "Nullable(Nothing)",
			output: &TypeDesc{
				Name:     "Nullable",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "Nothing",
						Nullable: false,
						Args:     []*TypeDesc{},
					},
				},
			},
		},
		{
			desc:  "empty arg",
			input: "DateTime()",
			output: &TypeDesc{
				Name:     "DateTime",
				Nullable: false,
				Args:     []*TypeDesc{},
			},
		},
		{
			desc:  "numeric arg",
			input: "FixedString(42)",
			output: &TypeDesc{
				Name:     "FixedString",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "42",
						Nullable: false,
						Args:     []*TypeDesc{},
					},
				},
			},
		},
		{
			desc:  "multiple args",
			input: "Array(Tuple(Tuple(String, String), Tuple(String, UInt64)))",
			output: &TypeDesc{
				Name:     "Array",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "Tuple",
						Nullable: false,
						Args: []*TypeDesc{
							{
								Name:     "Tuple",
								Nullable: false,
								Args: []*TypeDesc{
									{
										Name:     "String",
										Nullable: false,
										Args:     []*TypeDesc{},
									},
									{
										Name:     "String",
										Nullable: false,
										Args:     []*TypeDesc{},
									},
								},
							},
							{
								Name:     "Tuple",
								Nullable: false,
								Args: []*TypeDesc{
									{
										Name:     "String",
										Nullable: false,
										Args:     []*TypeDesc{},
									},
									{
										Name:     "UInt64",
										Nullable: false,
										Args:     []*TypeDesc{},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			desc:  "map args",
			input: "Map(String, Array(Int64))",
			output: &TypeDesc{
				Name:     "Map",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "String",
						Nullable: false,
						Args:     []*TypeDesc{},
					},
					{
						Name:     "Array",
						Nullable: false,
						Args: []*TypeDesc{
							{
								Name:     "Int64",
								Nullable: false,
								Args:     []*TypeDesc{},
							},
						},
					},
				},
			},
		},
		{
			desc:  "map nullable value args",
			input: "Map(String, String NULL)",
			output: &TypeDesc{
				Name:     "Map",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "String",
						Nullable: false,
						Args:     []*TypeDesc{},
					},
					{
						Name:     "String",
						Nullable: true,
						Args:     []*TypeDesc{},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(tt *testing.T) {
			output, err := ParseTypeDesc(tc.input)
			if tc.fail {
				assert.Error(tt, err)
			} else {
				assert.NoError(tt, err)
			}
			assert.Equal(tt, tc.output, output)
		})
	}
}

func TestParseComplexTypeWithNull(t *testing.T) {
	testCases := []*typeparserTestCase{
		{
			desc:  "complex nullable type",
			input: "Nullable(Tuple(String NULL, Array(Tuple(Array(Int32 NULL) NULL, Array(String NULL) NULL) NULL) NULL))",
			output: &TypeDesc{
				Name:     "Nullable",
				Nullable: false,
				Args: []*TypeDesc{
					{
						Name:     "Tuple",
						Nullable: false,
						Args: []*TypeDesc{
							{
								Name:     "String",
								Nullable: true,
								Args:     []*TypeDesc{},
							},
							{
								Name:     "Array",
								Nullable: true,
								Args: []*TypeDesc{
									{
										Name:     "Tuple",
										Nullable: true,
										Args: []*TypeDesc{
											{
												Name:     "Array",
												Nullable: true,
												Args: []*TypeDesc{
													{
														Name:     "Int32",
														Nullable: true,
														Args:     []*TypeDesc{},
													},
												},
											},
											{
												Name:     "Array",
												Nullable: true,
												Args: []*TypeDesc{
													{
														Name:     "String",
														Nullable: true,
														Args:     []*TypeDesc{},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(tt *testing.T) {
			output, err := ParseTypeDesc(tc.input)
			if tc.fail {
				assert.Error(tt, err)
			} else {
				assert.NoError(tt, err)
			}
			assert.Equal(tt, tc.output, output)
		})
	}
}
