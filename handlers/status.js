const HttpStatus = require("http-status");
const DynamoDBClient = require("../libs/dynamoDb-client.js");
const { handleSuccess, handleError } = require("../libs/response-handler.js");

exports.list = async (event) => {
  // Make sure body exist
  if (!event.pathParameters) throw new Error("Missing Parameter");

  try {
    // Retrieve notification_id from path parameter
    const notification_id = event.pathParameters.notification_id;

    if (!notification_id) {
      return handleError(
        HttpStatus.BAD_REQUEST,
        `[ListStatusLogs:List:Error]:${
          HttpStatus[HttpStatus.BAD_REQUEST]
        }: Invalid notification_id`
      );
    }

    const params = {
      TableName: process.env.DDB_STATUS_LOGS_TABLE_NAME,
      KeyConditionExpression: "notification_id = :notification_id",
      ExpressionAttributeValues: {
        ":notification_id": notification_id,
      },
    };

    // Get status logs record from StatusLogs dynamoDb table
    const result = await DynamoDBClient.query(params);

    if (result && !result.Items) {
      throw new Error("Logs not found.");
    }

    // Return success status code along with status logs  list
    return handleSuccess(result.Items);
  } catch (error) {
    return handleError(
      HttpStatus.INTERNAL_SERVER_ERROR,
      `[ListStatusLogs:List:Error]: ${error}`
    );
  }
};

exports.details = async (event) => {
  if (!event.pathParameters) throw new Error("Missing Parameter");

  try {
    // Retrieve notification_id and log_id from path parameter
    const { notification_id, log_id } = event.pathParameters;

    if (!notification_id || !log_id) {
      return handleError(
        HttpStatus.BAD_REQUEST,
        `[StatusLogs:Details:Error]:${
          HttpStatus[HttpStatus.BAD_REQUEST]
        }: "Invalid parameter"`
      );
    }

    const params = {
      TableName: process.env.DDB_STATUS_LOGS_TABLE_NAME,
      Key: {
        notification_id: notification_id,
        log_id: log_id,
      },
    };

    const result = await DynamoDBClient.get(params);

    if (result && !result.Item) {
      throw new Error("StatusLogs not found.");
    }

    return handleSuccess(result.Item);
  } catch (error) {
    return handleError(
      HttpStatus.INTERNAL_SERVER_ERROR,
      `[StatusLogs:Details:Error]: ${error}`
    );
  }
};

exports.summary = async (event) => {
  // Make sure body exist
  if (!event.pathParameters) throw new Error("Missing Parameter");

  try {
    // Retrieve notification_id from path parameter
    const notification_id = event.pathParameters.notification_id;

    if (!notification_id) {
      return handleError(
        HttpStatus.BAD_REQUEST,
        `[StatusLogs:Summary:Error]:${
          HttpStatus[HttpStatus.BAD_REQUEST]
        }: Invalid notification_id`
      );
    }

    const params = {
      TableName: process.env.DDB_STATUS_LOGS_TABLE_NAME,
      KeyConditionExpression: "notification_id = :notification_id",
      ExpressionAttributeValues: {
        ":notification_id": notification_id,
      },
    };

    // Get status logs record from StatusLogs dynamoDb table
    const result = await DynamoDBClient.query(params);

    if (result && !result.Items) {
      throw new Error("Logs not found.");
    }

    // Aggregate summary
    const summary = {};
    result.Items.reduce((acc, curr) => {
      return (summary[curr["delivery_status"]] =
        (summary[curr["delivery_status"]] || 0) + 1);
    }, summary);

    // Return success status code along with status logs  list
    return handleSuccess({
      total_notification_sent: result.Items.length,
      ...summary,
    });
  } catch (error) {
    return handleError(
      HttpStatus.INTERNAL_SERVER_ERROR,
      `[StatusLogs:Summary:Error]: ${error}`
    );
  }
};
