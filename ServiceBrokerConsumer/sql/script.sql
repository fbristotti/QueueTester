--alter database test set enable_broker
--go

if exists( select * from sys.services where name = 'CSharpService' )
	drop service CSharpService
go
if OBJECT_ID( 'CSharpQueue' ) is not null
	drop queue CSharpQueue
go
if exists( select * from sys.services where name = 'SqlToCSharpService' )
	drop service SqlToCSharpService
go
if OBJECT_ID( 'SqlToCSharpServiceQueue' ) is not null
	drop queue SqlToCSharpServiceQueue
go

create queue CSharpQueue
go
create service CSharpService on queue CSharpQueue ( [DEFAULT] )
go
create queue SqlToCSharpServiceQueue
go
create service SqlToCSharpService on queue SqlToCSharpServiceQueue ( [DEFAULT] )
go


DECLARE 
	@handle AS uniqueidentifier
,	@message varchar(max) = 'hello world!'
,	@conversation_group_id uniqueidentifier = '3EDF5736-8F85-45CF-A908-8EFAE334CE46'

begin tran
BEGIN DIALOG CONVERSATION @handle
FROM SERVICE SqlToCSharpService
TO SERVICE 'CSharpService'
WITH ENCRYPTION = OFF, RELATED_CONVERSATION_GROUP = @conversation_group_id;
    
SEND ON CONVERSATION @handle(@message)
commit
go


DECLARE 
	@handle AS uniqueidentifier
,	@message varchar(max) = 'hello world!'

waitfor(
	receive
	top(1)
		@handle = conversation_handle
	,	@message = convert(varchar, message_body)
	from CSharpQueue
),timeout 2000;
select @message;
end conversation @handle

go


DECLARE 
	@handle AS uniqueidentifier
,	@message varchar(max) = 'hello world!'

waitfor(
	receive
	top(1)
		@handle = conversation_handle
	,	@message = convert(varchar, message_body)
	from SqlToCSharpServiceQueue
	where conversation_group_id = '3EDF5736-8F85-45CF-A908-8EFAE334CE46'
),timeout 2000;
select @message;
end conversation @handle

select conversation_handle, conversation_group_id, message_type_name, convert(varchar, message_body) msgbody from CSharpQueue
select conversation_handle, conversation_group_id, message_type_name, convert(varchar, message_body) msgbody from SqltocsharpServiceQueue
SELECT conversation_handle, conversation_group_id, is_initiator, s.name as 'local service', 
far_service, sc.name 'contract', ce.state_desc
FROM sys.conversation_endpoints ce
LEFT JOIN sys.services s
ON ce.service_id = s.service_id
LEFT JOIN sys.service_contracts sc
ON ce.service_contract_id = sc.service_contract_id
where state_desc <> 'closed'


if OBJECT_ID( 'ExecuteInCSharp' ) IS NOT NULL
	drop procedure ExecuteInCSharp
GO

create procedure ExecuteInCSharp
	@message varchar(max) = 'hello world!'
as

DECLARE 
	@handle AS uniqueidentifier
,	@conversation_group_id uniqueidentifier = newid()

BEGIN DIALOG CONVERSATION @handle
FROM SERVICE SqlToCSharpService
TO SERVICE 'CSharpService'
WITH ENCRYPTION = OFF, RELATED_CONVERSATION_GROUP = @conversation_group_id;
    
SEND ON CONVERSATION @handle(@message)

waitfor(
	receive
	top(1)
		@handle = conversation_handle
	,	@message = convert(varchar, message_body)
	from SqlToCSharpServiceQueue
	where conversation_group_id = @conversation_group_id
),timeout -1;
end conversation @handle

return
go


exec ExecuteInCSharp 'fernando bristoti'


