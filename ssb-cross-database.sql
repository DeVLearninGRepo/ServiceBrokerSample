USE MASTER
GO
CREATE DATABASE SourceOrderDatabase
GO
ALTER DATABASE SourceOrderDatabase SET ENABLE_BROKER
GO
ALTER DATABASE SourceOrderDatabase SET TRUSTWORTHY ON
GO
CREATE MESSAGE TYPE [http://schemas.devlearning.com/ssb/OrderCreatedRequest] VALIDATION = NONE
GO
--CREATE MESSAGE TYPE [http://schemas.devlearning.com/ssb/OrderCreatedResponse] VALIDATION = NONE
--GO
CREATE CONTRACT [http://schemas.devlearning.com/ssb/OrderContract]
(
	 [http://schemas.devlearning.com/ssb/OrderCreatedRequest] SENT BY INITIATOR
)
GO
CREATE QUEUE dbo.OrderInitiatorQueue WITH STATUS = ON
GO
CREATE SERVICE OrderInitiatorService ON QUEUE dbo.OrderInitiatorQueue ([http://schemas.devlearning.com/ssb/OrderContract])
GO





USE MASTER
GO
CREATE DATABASE TargetOrderDatabase
GO
ALTER DATABASE TargetOrderDatabase SET ENABLE_BROKER
GO
USE TargetOrderDatabase
GO
ALTER DATABASE SourceOrderDatabase SET TRUSTWORTHY ON
GO
CREATE MESSAGE TYPE [http://schemas.devlearning.com/ssb/OrderCreatedRequest] VALIDATION = NONE
GO
CREATE CONTRACT [http://schemas.devlearning.com/ssb/OrderContract]
(
	 [http://schemas.devlearning.com/ssb/OrderCreatedRequest] SENT BY INITIATOR
)
GO
CREATE QUEUE dbo.OrderTargetQueue WITH STATUS = ON
GO
CREATE SERVICE OrderTargetService ON QUEUE dbo.OrderTargetQueue ([http://schemas.devlearning.com/ssb/OrderContract])
GO










USE SourceOrderDatabase
GO

BEGIN TRANSACTION

DECLARE @ConversationHandle UNIQUEIDENTIFIER
DECLARE @Message xml = '<order id="1" />'

BEGIN DIALOG CONVERSATION @ConversationHandle
FROM SERVICE OrderInitiatorService
TO SERVICE 'OrderTargetService'
ON CONTRACT [http://schemas.devlearning.com/ssb/OrderContract]
WITH ENCRYPTION = OFF;


SEND ON CONVERSATION @ConversationHandle
MESSAGE TYPE [http://schemas.devlearning.com/ssb/OrderCreatedRequest](@Message)

END CONVERSATION @ConversationHandle

COMMIT



USE SourceOrderDatabase
GO
SELECT * FROM sys.transmission_queue


USE TargetOrderDatabase
GO
SELECT * FROM sys.transmission_queue




USE MASTER
GO
SELECT TOP (1000) *, casted_message_body = 
CASE message_type_name WHEN 'X' 
  THEN CAST(message_body AS NVARCHAR(MAX)) 
  ELSE message_body 
END 
FROM SourceOrderDatabase.[dbo].orderinitiatorqueue WITH(NOLOCK)

SELECT TOP (1000) *, casted_message_body = 
CASE message_type_name WHEN 'X' 
  THEN CAST(message_body AS NVARCHAR(MAX)) 
  ELSE message_body 
END 
FROM TargetOrderDatabase.[dbo].[OrderTargetQueue] WITH(NOLOCK)


SELECT * FROM SourceOrderDatabase.sys.conversation_endpoints
SELECT * FROM TargetOrderDatabase.sys.conversation_endpoints


USE TargetOrderDatabase
GO

DECLARE @conversationHandle UNIQUEIDENTIFIER
DECLARE @cessageTypeName NVARCHAR(255)
DECLARE	@messageBody varbinary(MAX) 

BEGIN TRAN;

RECEIVE TOP(1) 
    @conversationHandle = conversation_handle,
    @messageTypeName = message_type_name,
	@messageBody = message_body
FROM dbo.OrderTargetQueue

IF @conversationHandle IS NOT NULL
BEGIN
    IF @messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
    BEGIN
        END CONVERSATION @conversationHandle;
    END
	SELECT convert(xml, @messageBody)
END
COMMIT TRANSACTION
