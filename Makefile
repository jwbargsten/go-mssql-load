
testdb:
	docker run --rm -p 1433:1433 -e "ACCEPT_EULA=1" -e "MSSQL_SA_PASSWORD=Passw0rd" mcr.microsoft.com/azure-sql-edge:latest
test:
	MSSQL_DSN="sqlserver://sa:Passw0rd@localhost:1433?database=master" go test -v ./...
