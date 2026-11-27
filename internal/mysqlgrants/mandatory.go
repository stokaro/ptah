package mysqlgrants

import "strings"

// MandatoryAccounts reads the value of MySQL's mandatory_roles variable. The
// setting has its own account-list grammar: spaces outside quotes disappear,
// quotes delimit text rather than escape each other, and an empty host means
// %. The returned names keep the setting's spelling, including host case.
// An empty user is omitted, as the server omits it when reading its setting.
// Duplicate accounts and accounts that do not exist are left to the caller.
//
// doubleQuotedLists selects MySQL 9.7's list grammar. Earlier releases protect
// commas and spaces inside single quotes and backticks, but not double quotes.
// Both grammars recognize all three quotes when separating user from host.
func MandatoryAccounts(value string, doubleQuotedLists bool) []Account {
	var accounts []Account
	var item strings.Builder
	var quote byte
	for i := range len(value) {
		b := value[i]
		switch {
		case quote != 0:
			item.WriteByte(b)
			if b == quote {
				quote = 0
			}
		case b == '\'' || b == '`' || b == '"' && doubleQuotedLists:
			quote = b
			item.WriteByte(b)
		case b == ',':
			accounts = appendMandatoryAccount(accounts, item.String())
			item.Reset()
		case b != ' ':
			item.WriteByte(b)
		}
	}
	return appendMandatoryAccount(accounts, item.String())
}

// appendMandatoryAccount reads one item after list separators and outside
// spaces have been removed. MySQL removes every delimiting quote, preserves
// backslashes, and treats each unquoted @ as a separator before host text.
func appendMandatoryAccount(accounts []Account, item string) []Account {
	var user, host strings.Builder
	part := &user
	var quote byte
	for i := range len(item) {
		b := item[i]
		switch {
		case quote != 0 && b == quote:
			quote = 0
		case quote != 0:
			part.WriteByte(b)
		case b == '\'' || b == '`' || b == '"':
			quote = b
		case b == '@':
			part = &host
		case b != ' ':
			part.WriteByte(b)
		}
	}
	if user.Len() == 0 {
		return accounts
	}
	account := Account{User: user.String(), Host: host.String()}
	if account.Host == "" {
		account.Host = "%"
	}
	return append(accounts, account)
}
