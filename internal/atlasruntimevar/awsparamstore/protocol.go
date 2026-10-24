package awsparamstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithytraits "github.com/aws/smithy-go/traits"
	"github.com/aws/smithy-go/transport/http/protocol/awsjson"
)

const (
	serviceNamespace  = "com.amazonaws.ssm"
	serviceID         = "SSM"
	operationName     = "GetParameter"
	signingName       = "ssm"
	serviceAPIVersion = "2014-11-06"
)

// GetParameterOutput is the GetParameter response a snapshot exposes through
// As, in the shape of the SDK's ssm.GetParameterOutput.
type GetParameterOutput struct {
	// Parameter is the parameter the service returned; nil when the response
	// carried none.
	Parameter *Parameter

	// ResultMetadata holds the SDK middleware metadata of the call, such as
	// the request ID.
	ResultMetadata middleware.Metadata
}

// Parameter is one Parameter Store parameter. A nil pointer is a member the
// response did not carry.
type Parameter struct {
	ARN              *string
	DataType         *string
	LastModifiedDate *time.Time
	Name             *string
	Selector         *string
	SourceResult     *string
	Type             string
	Value            *string
	Version          int64
}

// getParameterInput is the request body: the parameter name, and decryption,
// which the service ignores for a parameter that is not a SecureString.
type getParameterInput struct {
	name string
}

// protocolSchemas is the slice of the SSM model GetParameter touches: the
// service, the operation and its two structures, and every modeled error of
// the service, because the service may answer any operation with any of them.
//
// It is built on first use. The SDK's own client builds the schema of every
// shape of the service as package variables, which is the cost this package
// exists to avoid.
type protocolSchemas struct {
	protocol  *awsjson.Protocol
	operation *smithy.OperationSchema
	registry  *smithy.TypeRegistry

	inputName           *smithy.Schema
	inputWithDecryption *smithy.Schema

	result          *smithy.Schema
	resultParameter *smithy.Schema

	parameter                 *smithy.Schema
	parameterARN              *smithy.Schema
	parameterDataType         *smithy.Schema
	parameterLastModifiedDate *smithy.Schema
	parameterName             *smithy.Schema
	parameterSelector         *smithy.Schema
	parameterSourceResult     *smithy.Schema
	parameterType             *smithy.Schema
	parameterValue            *smithy.Schema
	parameterVersion          *smithy.Schema
}

var schemas = sync.OnceValue(buildSchemas)

func shapeID(name string) smithy.ShapeID {
	return smithy.ShapeID{Namespace: serviceNamespace, Name: name}
}

func buildSchemas() *protocolSchemas {
	stringShape := smithy.NewSchema(shapeID("String"), smithy.ShapeTypeString, 0)
	booleanShape := smithy.NewSchema(shapeID("Boolean"), smithy.ShapeTypeBoolean, 0)
	dateTime := smithy.NewSchema(shapeID("DateTime"), smithy.ShapeTypeTimestamp, 0)
	parameterName := smithy.NewSchema(shapeID("PSParameterName"), smithy.ShapeTypeString, 0)
	parameterValue := smithy.NewSchema(shapeID("PSParameterValue"), smithy.ShapeTypeString, 0, &smithytraits.Sensitive{})
	parameterVersion := smithy.NewSchema(shapeID("PSParameterVersion"), smithy.ShapeTypeLong, 0)
	parameterSelector := smithy.NewSchema(shapeID("PSParameterSelector"), smithy.ShapeTypeString, 0)
	parameterDataType := smithy.NewSchema(shapeID("ParameterDataType"), smithy.ShapeTypeString, 0)
	parameterType := smithy.NewSchema(shapeID("ParameterType"), smithy.ShapeTypeEnum, 0)

	s := &protocolSchemas{}

	input := smithy.NewSchema(shapeID("GetParameterRequest"), smithy.ShapeTypeStructure, 2)
	s.inputName = input.AddMember("Name", parameterName)
	s.inputWithDecryption = input.AddMember("WithDecryption", booleanShape)

	s.parameter = smithy.NewSchema(shapeID("Parameter"), smithy.ShapeTypeStructure, 9)
	s.parameterName = s.parameter.AddMember("Name", parameterName)
	s.parameterType = s.parameter.AddMember("Type", parameterType)
	s.parameterValue = s.parameter.AddMember("Value", parameterValue)
	s.parameterVersion = s.parameter.AddMember("Version", parameterVersion)
	s.parameterSelector = s.parameter.AddMember("Selector", parameterSelector)
	s.parameterSourceResult = s.parameter.AddMember("SourceResult", stringShape)
	s.parameterLastModifiedDate = s.parameter.AddMember("LastModifiedDate", dateTime)
	s.parameterARN = s.parameter.AddMember("ARN", stringShape)
	s.parameterDataType = s.parameter.AddMember("DataType", parameterDataType)

	s.result = smithy.NewSchema(shapeID("GetParameterResult"), smithy.ShapeTypeStructure, 1)
	s.resultParameter = s.result.AddMember("Parameter", s.parameter)

	operation := smithy.NewSchema(shapeID(operationName), smithy.ShapeTypeOperation, 0)
	s.operation = smithy.NewOperationSchema(operation, input, s.result)

	service := smithy.NewSchema(shapeID("AmazonSSM"), smithy.ShapeTypeService, 0,
		&smithytraits.XMLNamespace{URI: "http://ssm.amazonaws.com/doc/2014-11-06/"})
	s.protocol = awsjson.New11(smithy.NewServiceSchema(service, serviceAPIVersion))

	s.registry = &smithy.TypeRegistry{Entries: make(map[string]*smithy.TypeRegistryEntry, len(modeledErrors))}
	for _, shape := range modeledErrors {
		s.registry.Entries[serviceNamespace+"#"+shape.code] = shape.registryEntry(stringShape)
	}
	return s
}

// Serialize implements smithy.Serializable.
func (in *getParameterInput) Serialize(serializer smithy.ShapeSerializer) {
	s := schemas()
	serializer.WriteStruct(s.operation.Input)
	serializer.WriteString(s.inputName, in.name)
	serializer.WriteBool(s.inputWithDecryption, true)
	serializer.CloseStruct()
}

// Deserialize implements smithy.Deserializable.
func (out *GetParameterOutput) Deserialize(d smithy.ShapeDeserializer) error {
	s := schemas()
	return smithy.ReadStruct(d, s.result, func(member *smithy.Schema) error {
		if member != s.resultParameter {
			return nil
		}
		out.Parameter = &Parameter{}
		return out.Parameter.deserialize(d, s)
	})
}

func (p *Parameter) deserialize(d smithy.ShapeDeserializer, s *protocolSchemas) error {
	return smithy.ReadStruct(d, s.parameter, func(member *smithy.Schema) error {
		switch member {
		case s.parameterARN:
			p.ARN = new(string)
			return d.ReadString(member, p.ARN)
		case s.parameterDataType:
			p.DataType = new(string)
			return d.ReadString(member, p.DataType)
		case s.parameterLastModifiedDate:
			p.LastModifiedDate = new(time.Time)
			return d.ReadTime(member, p.LastModifiedDate)
		case s.parameterName:
			p.Name = new(string)
			return d.ReadString(member, p.Name)
		case s.parameterSelector:
			p.Selector = new(string)
			return d.ReadString(member, p.Selector)
		case s.parameterSourceResult:
			p.SourceResult = new(string)
			return d.ReadString(member, p.SourceResult)
		case s.parameterType:
			return d.ReadString(member, &p.Type)
		case s.parameterValue:
			p.Value = new(string)
			return d.ReadString(member, p.Value)
		case s.parameterVersion:
			return d.ReadInt64(member, &p.Version)
		}
		return nil
	})
}

// errorShape is one modeled error of the SSM service: its code, its fault,
// and its message member as the model spells it, "Message" or "message". The
// JSON reader accepts either capitalization for that member. A shape without
// one, the empty name, never carries a message into its error text.
type errorShape struct {
	code          string
	fault         smithy.ErrorFault
	messageMember string
}

func (e errorShape) registryEntry(stringShape *smithy.Schema) *smithy.TypeRegistryEntry {
	numMembers := 0
	if e.messageMember != "" {
		numMembers = 1
	}
	schema := smithy.NewSchema(shapeID(e.code), smithy.ShapeTypeStructure, numMembers,
		&smithytraits.Error{Type: faultName(e.fault)})
	var message *smithy.Schema
	if e.messageMember != "" {
		message = schema.AddMember(e.messageMember, stringShape)
	}
	return &smithy.TypeRegistryEntry{
		Schema: schema,
		New: func() any {
			return &ServiceError{code: e.code, fault: e.fault, schema: schema, messageMember: message}
		},
	}
}

func faultName(fault smithy.ErrorFault) string {
	if fault == smithy.FaultServer {
		return "server"
	}
	return "client"
}

// ServiceError is a modeled SSM error, such as ParameterNotFound. It
// implements smithy.APIError, and its text is the SDK's for the same error:
// the code, a colon, and the message.
type ServiceError struct {
	// Message is the message the response carried; nil when it carried none.
	Message *string

	code          string
	fault         smithy.ErrorFault
	schema        *smithy.Schema
	messageMember *smithy.Schema
}

// Error implements error.
func (e *ServiceError) Error() string {
	return fmt.Sprintf("%s: %s", e.ErrorCode(), e.ErrorMessage())
}

// ErrorCode implements smithy.APIError.
func (e *ServiceError) ErrorCode() string {
	return e.code
}

// ErrorMessage implements smithy.APIError.
func (e *ServiceError) ErrorMessage() string {
	if e.Message == nil {
		return ""
	}
	return *e.Message
}

// ErrorFault implements smithy.APIError.
func (e *ServiceError) ErrorFault() smithy.ErrorFault {
	return e.fault
}

// Deserialize implements smithy.Deserializable. Only the message member is
// read; the other members a few shapes carry never reach the error text.
func (e *ServiceError) Deserialize(d smithy.ShapeDeserializer) error {
	return smithy.ReadStruct(d, e.schema, func(member *smithy.Schema) error {
		if member != e.messageMember {
			return nil
		}
		e.Message = new(string)
		return d.ReadString(member, e.Message)
	})
}
