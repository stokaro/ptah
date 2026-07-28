package main

import "fmt"

func main() {
	fmt.Print(`CREATE OR ALTER FUNCTION dbo.score(@value int)
RETURNS int
AS
BEGIN
	RETURN @value;
END
GO
CREATE TABLE widgets (
	id int NOT NULL,
	CONSTRAINT pk_widgets PRIMARY KEY (id)
);
GO
`)
}
