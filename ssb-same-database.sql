USE MASTER
GO
CREATE DATABASE OrderDatabase
GO
ALTER DATABASE OrderDatabase SET ENABLE_BROKER
GO
USE OrderDatabase
GO
CREATE MESSAGE TYPE [http://schemas.devlearning.com/ssb/OrderCreatedRequest] VALIDATION = NONE
GO
CREATE CONTRACT [http://schemas.devlearning.com/ssb/OrderContract]
(
	 [http://schemas.devlearning.com/ssb/OrderCreatedRequest] SENT BY INITIATOR
)
GO
CREATE QUEUE dbo.OrderInitiatorQueue WITH STATUS = ON
GO
CREATE SERVICE OrderInitiatorService ON QUEUE dbo.OrderInitiatorQueue ([http://schemas.devlearning.com/ssb/OrderContract])
GO
CREATE PROCEDURE [dbo].[OrderReadFromQueue]
AS
BEGIN
	
	BEGIN TRANSACTION


	ROLLBACK TRANSACTION

END
CREATE QUEUE [dbo].[OrderTargetQueue]  WITH STATUS = ON, RETENTION = ON, ACTIVATION (STATUS = ON, PROCEDURE_NAME = [dbo].[OrderReadFromQueue], MAX_QUEUE_READERS = 1, EXECUTE AS OWNER), POISON_MESSAGE_HANDLING (STATUS = ON)
GO
CREATE SERVICE OrderTargetService ON QUEUE dbo.OrderTargetQueue ([http://schemas.devlearning.com/ssb/OrderContract])
GO

CREATE TABLE dbo.TestQueue
(
	Id UNIQUEIDENTIFIER DEFAULT NEWID(),
	Message XML,
	PRIMARY KEY (Id)
)
GO

ALTER PROCEDURE [dbo].[OrderReadFromQueue]
AS

DECLARE @conversationHandle UNIQUEIDENTIFIER
DECLARE	@messageTypeName nvarchar(256)
DECLARE	@messageBody varbinary(MAX) 
	
WHILE(1=1)
BEGIN
	BEGIN TRY
		BEGIN TRANSACTION

		SELECT
			@conversationHandle = null,
			@messageTypeName = null,
			@messageBody = null
		
		WAITFOR
		(
			RECEIVE TOP(1)
				@conversationHandle = conversation_handle,
				@messageTypeName = message_type_name,
				@messageBody = message_body
			FROM dbo.OrderTargetQueue
		), TIMEOUT 1000


		IF @conversationHandle is null
		BEGIN
			COMMIT TRANSACTION
			BREAK
		END

		IF @messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog'
			OR @messageTypeName = 'http://schemas.microsoft.com/SQL/ServiceBroker/Error'
		BEGIN
			END CONVERSATION @conversationHandle
		END
		ELSE IF @messageTypeName = 'http://schemas.devlearning.com/ssb/OrderCreatedRequest'
		BEGIN
			DECLARE @xmlMessage XML = convert(xml, @messageBody)

			WAITFOR DELAY '00:00:10'

			INSERT INTO dbo.TestQueue
			(
			    Message
			)
			VALUES
			(
				@xmlMessage
			)
		END
		ELSE
		BEGIN
			RAISERROR('Unmanaged messageType', 10, 1)
		END
		
		COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
		PRINT ERROR_MESSAGE()
	
		if @@TRANCOUNT > 0
			ROLLBACK TRANSACTION
	
		break
	end catch
END
GO




BEGIN TRANSACTION

DECLARE @ConversationHandle UNIQUEIDENTIFIER
DECLARE @Message xml = '<order id="2" />'

BEGIN DIALOG CONVERSATION @ConversationHandle
FROM SERVICE OrderInitiatorService
TO SERVICE 'OrderTargetService'
ON CONTRACT [http://schemas.devlearning.com/ssb/OrderContract]
WITH ENCRYPTION = OFF;


SEND ON CONVERSATION @ConversationHandle
MESSAGE TYPE [http://schemas.devlearning.com/ssb/OrderCreatedRequest](@Message)

END CONVERSATION @ConversationHandle

COMMIT


SELECT * FROM dbo.TestQueue