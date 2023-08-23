import datetime
import json
import logging
import os
import time
from json import JSONDecodeError
from typing import List, Optional, Dict, Any
from xml.dom import minidom
from xml.etree import ElementTree

import boto3
import boto3.dynamodb.table
from boto3.dynamodb.conditions import Key
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from chalice import Chalice, BadRequestError, Response
from chalice.app import Request, LambdaFunctionEvent

from chalicelib.entities.destination import Destination
from chalicelib.entities.gcp_credentials import GcpCredentials
from chalicelib.entities.indices_check_status import IndicesCheckStatus, \
    IndexStatus
from chalicelib.entities.makewithtech_numbers import MakeWithTechNumbers
from chalicelib.entities.requests import BulkUpdateModelsRequest, SendMailRequest, SendSlackMessageRequest
from chalicelib.entities.secrets import Secrets
from chalicelib.facades.database_facade import DatabaseFacade, MemoryDatabaseFacade, DynamoDBFacade, UsageMetric
from chalicelib.facades.opensearch_facade import SearchableIndexFacade, \
    StubbedSearchableIndexFacade, OpenSearchFacade
from chalicelib.facades.sitemap_storage_facade import StubbedSitemapStorageFacade, AmazonS3SitemapStorageFacade, \
    SitemapStorageFacade
from chalicelib.facades.stripe_facade import StripeFacade, StubbedStripeFacade
from chalicelib.utils.cloud_utils import CloudUtils, StubbedCloudUtils, SearchProfileParams, UpdateProfileParams
from chalicelib.utils.feature_toggles import FeatureToggles
from chalicelib.utils.responder import Responder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Chalice(app_name=os.environ.get("APP_NAME", "mwt-utils-backend"))

REGION = os.environ.get("REGION")
STAGE = os.environ.get("STAGE", "dev")
logger.info(f"Using region: {REGION}, stage: {STAGE}")

SECRETS = None
DATABASE_FACADE = None


@app.route("/ping", methods=["GET"])
def ping() -> Dict[str, str]:
    return {"value": "pong"}


@app.schedule("rate(5 minutes)")
def check_indices(event: LambdaFunctionEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")

    if FeatureToggles.on(FeatureToggles.DISABLE_CHECK_INDICES):
        logger.info("Check indices: disabled")
        return

    entities = os.environ["ENTITIES"].split(",")
    logger.info(f"Checking the following entities: {entities}")
    indices_status = IndicesCheckStatus.instance()

    for entity in entities:
        logger.info(f"Making sure that index for {entity} is complete.")

        item_count = _count_items(entity_name=entity)
        logger.info(f"Found {item_count} DB items for {entity}.")

        document_count = _count_documents(entity_name=entity)
        if not document_count:
            logger.warning(
                f"Skipping {entity} as we cannot count the documents.")
            continue

        logger.info(f"Found {document_count} document(s) for {entity}.")

        indices_status.entities[entity] = IndexStatus(
            item_count=item_count, document_count=document_count)

    for entity_name, status in indices_status.entities.items():
        if status.item_count != status.document_count:
            logger.info(f"Found mismatch for {entity_name}. Updating index.")
            if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
                logger.info("Skipping in test context ..")
                continue

            function_name = f"backfill_{entity_name.lower()}"
            logger.info(f"Looking for reindex function called {function_name}.")
            reindex_function = globals().get(function_name)
            if reindex_function:
                logger.warning("Calling reindex function.")
                reindex_function()
            else:
                logger.warning(f"Unable to find function: {function_name}")

        else:
            logger.info(f"OpenSearch index for {entity_name} is up to date.")


@app.schedule("rate(15 minutes)")
def align_comments_and_favourites(event: LambdaFunctionEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")

    if FeatureToggles.on(FeatureToggles.DISABLE_ALIGN_COMMENTS):
        logger.info("Align comments: disabled")
        return

    models_table_name = os.environ["DYNAMO_TABLE_NAME_MODELS"]
    logger.info(f"Aligning models in Dynamo model_table: {models_table_name} ..")
    dynamodb_resource = _dynamodb_resource()
    model_table = dynamodb_resource.Table(models_table_name)
    models_response = model_table.scan()
    data = models_response["Items"]

    while "LastEvaluatedKey" in models_response:
        logger.info(f"Scanning next page after {len(data)} results ..")
        models_response = model_table.scan(ExclusiveStartKey=models_response["LastEvaluatedKey"])
        data.extend(models_response["Items"])

    logger.info(f"Found {len(data)} models ...")

    if not data:
        return

    favorites_table_name = os.environ["DYNAMO_TABLE_NAME_FAVORITES"]
    logger.info(f"Favorites in Dynamo are in table: {favorites_table_name} ..")
    favorites_table = dynamodb_resource.Table(favorites_table_name)

    comments_table_name = os.environ["DYNAMO_TABLE_NAME_COMMENTS"]
    logger.info(f"Comments in Dynamo are in table: {comments_table_name} ..")
    comments_table = dynamodb_resource.Table(comments_table_name)

    for item in data:
        item_id = item["id"]
        model_id = item.get("modelID")
        if not model_id:
            continue

        new_favorites_count = None
        new_comment_count = None

        # fetch favorites
        try:
            favorites_response = favorites_table.query(
                KeyConditionExpression=Key("modelID").eq(model_id),
                IndexName="mWTFavoritesByModelID"
            )
            favorites_count = len(favorites_response.get("Items", []))
            if favorites_count != item["favoriteCnt"]:
                new_favorites_count = int(favorites_count)
        except ClientError as e:
            error_message = e.response.get("Error", {}).get("Message")
            logger.exception(
                f"Unable to fetch favorites for model {model_id}: %s" % error_message
            )

        # fetch comments
        try:
            comments_response = comments_table.query(
                KeyConditionExpression=Key("sortBase").eq("dummyvalueforsort") & Key("modelID").eq(model_id),
                IndexName="byModelID"
            )
            comments_count = len(comments_response.get("Items", []))
            if comments_count != item["commentCnt"]:
                new_comment_count = int(comments_count)
        except ClientError as e:
            error_message = e.response.get("Error", {}).get("Message")
            logger.exception(
                f"Unable to fetch comments for model {model_id}: %s" % error_message
            )

        # potentially update model table
        if new_favorites_count is not None or new_comment_count is not None:
            logger.info(f"Updating model {model_id} ...")

            expressions = []
            attr_values = {}
            if new_favorites_count:
                logger.info(f".. favorites will be set to: {new_favorites_count})")

                expressions.append("favoriteCnt = :newFavoriteCnt")
                attr_values[":newFavoriteCnt"] = int(new_favorites_count)

            if new_comment_count:
                logger.info(f".. comments will be set to: {new_favorites_count})")
                expressions.append("commentCnt = :newCommentCount")
                attr_values[":newCommentCount"] = int(new_comment_count)

            try:
                model_table.update_item(
                    Key={"id": item_id},
                    UpdateExpression=f'SET {", ".join(expressions)}',
                    ExpressionAttributeValues=attr_values,
                    ReturnValues="UPDATED_NEW"
                )
                logger.info("Model updated.")
            except ClientError as e:
                error_message = e.response.get("Error", {}).get("Message")
                logger.exception(
                    f"Unable to update model {model_id}: %s" % error_message
                )

    logger.info(f"Done aligning comments and favorites for {len(data)} models.")


@app.schedule("rate(1 hour)")
def sync_subscriptions(event: LambdaFunctionEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")

    if FeatureToggles.on(FeatureToggles.DISABLE_SYNC_SUBSCRIPTIONS):
        logger.info("Sync subscriptions: disabled")
        return

    secrets = _secrets()
    stripe_facade = _stripe_facade(secrets=secrets)
    cloud_utils = _cloud_utils(secrets=secrets)

    dangling_active = []
    dangling_cancelled = []

    summary = {
        "active": {"total": 0, "updated": 0, "dangling": 0},
        "cancelled": {"total": 0, "updated": 0, "dangling": 0},
    }

    active_subscriptions = stripe_facade.active_subscriptions()
    logger.info(f"Found {len(active_subscriptions)} active subscriptions.")
    summary["active"]["total"] = len(active_subscriptions)

    for active_subscription in active_subscriptions:
        owner = cloud_utils.search_profiles(params=SearchProfileParams(subscription_id=active_subscription.id))
        if owner is None:
            dangling_active.append(active_subscription.id)
            continue

        subscription_id = owner.subscription_id
        subscription_name = owner.subscription_name
        subscription_expiration = owner.subscription_expiration

        nickname = active_subscription.nickname()
        ends_at = active_subscription.ends_at()
        if subscription_name != nickname or subscription_expiration != ends_at:
            logger.info(f"Found mismatch. Updating user profile {owner.nickname}")
            params = UpdateProfileParams(id=owner.id, subscription_id=subscription_id, subscription_expiration=ends_at,
                                         subscription_name=nickname)
            cloud_utils.update_profile(params=params)
            summary["active"]["updated"] += 1

    cancelled_subscriptions = stripe_facade.cancelled_subscriptions()
    logger.info(f"Found {len(cancelled_subscriptions)} cancelled subscriptions.")
    summary["cancelled"]["total"] = len(cancelled_subscriptions)

    for cancelled in cancelled_subscriptions:
        owner = cloud_utils.search_profiles(params=SearchProfileParams(subscription_id=cancelled.id))
        if owner is None:
            dangling_cancelled.append(cancelled.id)
            continue

        if int(time.time()) < cancelled.current_period_end:
            logger.info(f"Subscription {cancelled.id} is cancelled but ends at {cancelled.ends_at()}")

    summary["active"]["dangling"] = len(dangling_active)
    summary["cancelled"]["dangling"] = len(dangling_cancelled)
    logger.info(summary)

    if channel_id := os.environ.get("SLACK_NOTIFICATIONS_CHANNEL_ID"):
        if dangling_active:
            message = f"Unable to search for active subscription owners: {','.join(dangling_active)}"
            cloud_utils.send_slack_message(message=message, channel_id=channel_id)

        if dangling_cancelled:
            message = f"Unable to search for cancelled subscription owners: {','.join(dangling_cancelled)}"
            cloud_utils.send_slack_message(message=message, channel_id=channel_id)


@app.schedule("rate(1 hour)")
def purge_render_log(event: LambdaFunctionEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")

    if FeatureToggles.on(FeatureToggles.DISABLE_PURGE_RENDER_LOG):
        logger.info("Purge render log: disabled")
        return

    dynamodb_resource = _dynamodb_resource()

    ddb_table_name = os.environ.get("DYNAMO_TABLE_NAME_RENDER_LOG")
    logger.info(f"Using DynamoDB table name: {ddb_table_name}")

    table = dynamodb_resource.Table(ddb_table_name)
    logger.info(f"Found table: {table}")

    page_limit = int(os.environ.get("PURGE_RENDER_LOG_PAGE_LIMIT", "100"))
    purge_limit = int(os.environ.get("PURGE_RENDER_LOG_PURGEABLE_LIMIT", "100"))
    allowed_age_seconds = int(os.environ.get("PURGE_RENDER_LOG_ALLOWED_AGE_SECONDS", "604800"))

    purgeable_items = list()

    response = None
    while True:
        try:
            if not response:
                response = table.scan(Limit=page_limit)
            else:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                    Limit=page_limit
                )
        except ClientError as e:
            logger.exception(
                f"Error scanning {ddb_table_name}: "
                f"{e.response.get('Error', {}).get('Message')}"
            )
            raise ValueError("Failed to purge render log (DynamoDB error).")

        for item in response["Items"]:
            item_id = item.get("id")
            request_id = item.get("requestId")
            created_at_iso = item.get("createdAt")
            if created_at_iso:
                if created_at_iso.endswith("Z"):
                    created_at_iso = created_at_iso[:-1]
                try:
                    created_at = datetime.datetime.fromisoformat(created_at_iso)
                except ValueError:
                    logger.warning(f"Invalid 'createdAt' value ({created_at_iso}) in item: {item_id}")
                    continue

                now = int(time.time())
                age_seconds = now - int(created_at.timestamp())
                if age_seconds > allowed_age_seconds:
                    purgeable_items.append((item_id, request_id))
                    if len(purgeable_items) == purge_limit:
                        break

        if len(purgeable_items) == purge_limit:
            logger.info(f"Found enough records ({purge_limit}) to purge for this round ..")
            break

        if "LastEvaluatedKey" not in response:
            logger.info("No additional records to scan ..")
            break

    if purgeable_items:
        logger.info(f"About to purge {len(purgeable_items)} item(s) ..")
        with table.batch_writer() as batch:
            for (purgeable_id, purgeable_request_id) in purgeable_items:
                batch.delete_item(
                    Key={
                        "id": purgeable_id,
                        "requestId": purgeable_request_id
                    }
                )

    logger.info("Done purging render logs.")


@app.schedule("rate(1 hour)")
def generate_sitemap(event: LambdaFunctionEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")

    if FeatureToggles.on(FeatureToggles.DISABLE_GENERATE_SITEMAP):
        logger.info("Generate sitemap: disabled")
        return

    model_entity = "MODELS"
    logger.info(f"Generating sitemap for {model_entity}.")

    sitemap_storage_facade = _sitemap_storage_facade()

    items = sorted(_db_items(entity_name=model_entity), key=lambda db_item: db_item["id"])
    logger.info(f"Found {len(items)} model(s). Excluding private and hidden models ...")

    def restricted(item: Dict[str, Any]) -> bool:
        access_control = item.get("accessControl")
        is_private = access_control and access_control == "private"

        hidden = item.get("hidden")
        is_hidden = hidden is not None and hidden is True

        return is_private or is_hidden

    items = [item for item in items if not restricted(item)]
    logger.info(f"Found {len(items)} model(s) that are not restricted ...")

    if not items:
        return

    root = ElementTree.Element("urlset")
    root.attrib["xmlns"] = "http://www.sitemaps.org/schemas/sitemap/0.9"

    static_pages = [
        "https://models.makewithtech.com/",
        "https://models.makewithtech.com/models",
        "https://models.makewithtech.com/thingiversebrowser",
        "https://models.makewithtech.com/openscad-editor",
        "https://models.makewithtech.com/share",
        "https://models.makewithtech.com/listcompilequeue",
        "https://models.makewithtech.com/listrenderedmodels",
        "https://models.makewithtech.com/help",
        "https://models.makewithtech.com/about",
        "https://models.makewithtech.com/customizemodels",
        "https://models.makewithtech.com/creatorbenefits",
        "https://models.makewithtech.com/reportbug",
    ]

    for static_page in static_pages:
        doc = ElementTree.SubElement(root, "url")
        ElementTree.SubElement(doc, "loc").text = static_page

    hostname = os.environ["WEBSITE_HOSTNAME"]
    for item in items:
        model_url = f"https://{hostname}/models/{item['id']}"

        doc = ElementTree.SubElement(root, "url")
        ElementTree.SubElement(doc, "loc").text = model_url

    xml = minidom.parseString(ElementTree.tostring(root)).toprettyxml(indent="   ")
    sitemap_storage_facade.store_sitemap(sitemap_xml=xml)


@app.route("/v1/users/{user_id}", methods=["GET"], cors=True)
def users(user_id: str) -> Response:
    request = app.current_request
    logger.info(f"GET /v1/users/{user_id}. Headers: {request.headers}")

    profile_table_name = os.environ["DYNAMO_TABLE_NAME_PROFILES"]
    dynamodb_resource = _dynamodb_resource()
    profile_table = dynamodb_resource.Table(profile_table_name)

    try:
        logger.info(f"Looking up {user_id} in table {profile_table_name}")
        response = profile_table.query(
            KeyConditionExpression=Key("id").eq(user_id)
        )
        items = response.get("Items", [])
    except ClientError as e:
        error_message = e.response.get("Error", {}).get("Message")
        logger.exception(
            f"Unable to fetch profile {user_id}: %s" % error_message
        )
        return Responder.error(error_message="Unable to fetch user.", status_code=500)

    if not items or "cognitoUsername" not in items[0]:
        logger.info(f"User {user_id} not found or 'cognitoUsername' attribute missing.")
        return Responder.error(error_message="User not found.", status_code=404)

    username = items[0]["cognitoUsername"]
    user_pool_id = os.environ["COGNITO_USER_POOL_ID"]

    cloud_utils = _cloud_utils(secrets=None)

    logger.info(f"About to fetch user {username} from pool {user_pool_id}")
    userdata = cloud_utils.pool_user(user_pool_id=user_pool_id, username=username)
    return Responder.respond(payload=userdata or {})


@app.route("/v1/mails", methods=["POST"])
def send_mail() -> Response:
    request = app.current_request
    logger.info(f"POST /v1/mails. Body: {request.raw_body}, Headers: {request.headers}")

    if validation_failure_response := _validate_api_key(request):
        return validation_failure_response

    try:
        mail_request = SendMailRequest.from_dict(request.json_body)
    except (BadRequestError, LookupError, AttributeError, JSONDecodeError):
        logger.warning(f"Request isn't valid JSON denoting a SendMailRequest: {request.raw_body}")
        return Responder.error(
            status_code=400,
            error_message=f"Request invalid. ({request.raw_body})"
        )

    secrets = _secrets()
    cloud_utils = _cloud_utils(secrets=secrets)
    try:
        message_id = cloud_utils.send_email(
            sender_address="qa@cogitations.com",
            recipient=Destination(recipients=mail_request.recipients),
            subject=mail_request.subject,
            text=mail_request.text
        )
        return Responder.respond(
            payload={"status": "success", "message_id": message_id}
        )

    except ValueError:
        return Responder.error(
            status_code=500,
            error_message="Failed to send email."
        )


@app.route("/v1/slack/makewithtech/messages", methods=["POST"])
def send_slack_message() -> Response:
    request = app.current_request
    logger.info(f"POST /v1/slack/makewithtech/messages. Body: {request.raw_body}, Headers: {request.headers}")

    if validation_failure_response := _validate_api_key(request):
        return validation_failure_response

    try:
        message_request = SendSlackMessageRequest.from_dict(request.json_body)
    except (BadRequestError, LookupError, AttributeError, JSONDecodeError):
        logger.warning(f"Request isn't valid JSON denoting a SendSlackMessageRequest: {request.raw_body}")
        return Responder.error(
            status_code=400,
            error_message=f"Request invalid. ({request.raw_body})"
        )

    secrets = _secrets()
    cloud_utils = _cloud_utils(secrets=secrets)
    try:
        cloud_utils.send_slack_message(message=message_request.message, channel_id=message_request.channel)
        return Responder.respond(
            payload={"status": "success", "message_id": "unknown"}
        )

    except ValueError:
        return Responder.error(
            status_code=500,
            error_message="Failed to send Slack message."
        )


@app.schedule("cron(0 5 * * ? *)")
def poll_stats(event: LambdaFunctionEvent) -> None:
    logger.info(f"Received event: {event.to_dict()}")

    if FeatureToggles.on(FeatureToggles.DISABLE_POLL_STATS):
        logger.info("Poll stats: disabled")
        return

    secrets = _secrets()
    cloud_utils = _cloud_utils(secrets=secrets)
    database_facade = _database_facade()

    previous_usage = database_facade.fetch_latest_usage_metric()
    datetime_now = datetime.datetime.fromtimestamp(int(time.time()))
    current_usage = UsageMetric(
        timestamp=datetime_now.isoformat(),
        cognito_users=0, user_profiles=0, active_subscriptions=0, public_models=0, private_models=0
    )

    if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
        current_usage.public_models = 10
        current_usage.private_models = 10
        current_usage.user_profiles = 10
    else:
        models = _db_items(entity_name="MODELS")
        for model in models:
            private_model = model.get("private", False)
            if private_model is True or private_model == "True":
                current_usage.private_models += 1
            else:
                current_usage.public_models += 1

        current_usage.user_profiles = _count_items(entity_name="PROFILES")

    stripe_facade = _stripe_facade(secrets=secrets)
    active_subscriptions = stripe_facade.active_subscriptions()
    current_usage.active_subscriptions = len(active_subscriptions)

    user_pool = os.environ.get("COGNITO_USER_POOL_ID")

    if not user_pool:
        logger.info("No user pool configured.")
    else:
        logger.info("Counting users.")
        current_usage.cognito_users = cloud_utils.count_pool_users(user_pool_id=user_pool)

    logger.info(f"Current usage: {current_usage.to_dict()}")
    if previous_usage:
        logger.info(f"Previous usage: {previous_usage.to_dict()}")

    try:
        database_facade.store_usage_metric(usage_metric=current_usage)
    except ValueError:
        logger.exception("Unable to send Slack messages.")

    channel_id = os.environ["SLACK_NOTIFICATIONS_CHANNEL_ID"]

    total_models = current_usage.public_models + current_usage.private_models
    total_models_before = 0
    active_subscriptions_before = 0
    cognito_before = 0
    profiles_before = 0
    if previous_usage:
        total_models_before = previous_usage.public_models + previous_usage.private_models
        active_subscriptions_before = previous_usage.active_subscriptions
        cognito_before = previous_usage.cognito_users
        profiles_before = previous_usage.user_profiles

    delta_total_models = total_models - total_models_before
    delta_subscribers = current_usage.active_subscriptions - active_subscriptions_before
    delta_cognito = current_usage.cognito_users - cognito_before
    delta_profiles = current_usage.user_profiles - profiles_before

    def percentage(first: int, second: int) -> Optional[float]:
        if first and second and first >= second:
            try:
                return 100.0 / float(first) * float(second)
            except (ValueError, ZeroDivisionError):
                logger.warning(f"Unable to calculate percentage: {first}, {second}")
        return None

    percent_verified_users = percentage(current_usage.cognito_users, current_usage.user_profiles)
    percent_subscribers = percentage(current_usage.user_profiles, current_usage.active_subscriptions)

    makewithtech_numbers = MakeWithTechNumbers(
        day=datetime_now.strftime("%Y-%m-%d"),
        total_models=total_models,
        delta_total_models=delta_total_models,
        public_models=current_usage.public_models,
        private_models=current_usage.private_models,
        registered_user_count=current_usage.cognito_users,
        delta_registered_user_count=delta_cognito,
        total_user_profiles=current_usage.user_profiles,
        delta_total_user_profiles=delta_profiles,
        verified_user_ratio=percent_verified_users,
        subscribers=current_usage.active_subscriptions,
        delta_subscribers=delta_subscribers,
        subscriber_ratio=percent_subscribers
    )
    makewithtech_numbers.report_on_slack(datetime_now=datetime_now, cloud_utils=cloud_utils, channel_id=channel_id)
    makewithtech_numbers.report_on_spreadsheet(datetime_now=datetime_now, cloud_utils=cloud_utils)


@app.route("/v1/backfill/all", methods=["GET"])
def backfill_all() -> Dict[str, Any]:
    logger.info("Running GET /v1/backfill/all")
    lambda_client = boto3.client("lambda", region_name=REGION)
    profiles = _backfill_profiles(lambda_client=lambda_client)
    models = _backfill_models(lambda_client=lambda_client)
    return {
        "status": "done",
        "profiles_backfilled": profiles,
        "models_backfilled": models,
    }


@app.route("/v1/backfill/profiles", methods=["GET"])
def backfill_profiles() -> Dict[str, Any]:
    logger.info("Running GET /v1/backfill/profiles")
    lambda_client = boto3.client("lambda", region_name=REGION)
    profiles = _backfill_profiles(lambda_client=lambda_client)
    return {
        "status": "done",
        "profiles_backfilled": profiles
    }


@app.route("/v1/backfill/models", methods=["GET"])
def backfill_models() -> Dict[str, Any]:
    logger.info("Running GET /v1/backfill/models")
    lambda_client = boto3.client("lambda", region_name=REGION)
    models = _backfill_models(lambda_client=lambda_client)
    return {
        "status": "done",
        "models_backfilled": models
    }


@app.route("/v1/bulkupdate/models", methods=["POST"], cors=True)
def bulk_update_models() -> Response:
    request = app.current_request
    logger.info(f"POST /v1/bulkupdate/models. Body: {request.raw_body}, Headers: {request.headers}")

    if validation_failure_response := _validate_api_key(request):
        return validation_failure_response

    try:
        update_request = BulkUpdateModelsRequest.from_dict(request.json_body)
    except (BadRequestError, LookupError, AttributeError, JSONDecodeError):
        logger.warning(f"Request isn't valid JSON denoting a BulkUpdateModelsRequest: {request.raw_body}")
        return Responder.error(
            status_code=400,
            error_message=f"Request invalid. ({request.raw_body})"
        )

    model_entity = "MODELS"
    model_items = _db_items(entity_name=model_entity)
    logger.info(f"Found {len(model_items)} models.")

    updated = 0

    dynamodb_resource = _dynamodb_resource()
    models_table_name = os.environ["DYNAMO_TABLE_NAME_MODELS"]
    model_table = dynamodb_resource.Table(models_table_name)

    for item in model_items:
        model_id = item.get("id")

        expressions = []
        attr_values = {}

        if update_request.creator:
            update_from = update_request.creator.change_from
            update_to = update_request.creator.change_to
            if update_from and update_to:
                creator = item.get("creator")
                if update_from == creator:
                    logger.info(f"For model {model_id}, updating creator (was: {update_from}, to: {update_to})")

                    expressions.append("creator = :newCreator")
                    attr_values[":newCreator"] = str(update_to)

        if expressions:
            try:
                model_table.update_item(
                    Key={"id": model_id},
                    UpdateExpression=f'SET {", ".join(expressions)}',
                    ExpressionAttributeValues=attr_values,
                    ReturnValues="UPDATED_NEW"
                )
                logger.info("Model updated.")
            except ClientError as e:
                error_message = e.response.get("Error", {}).get("Message")
                logger.exception(
                    f"Unable to update model {model_id}: %s" % error_message
                )

            updated += 1

    return Responder.respond(payload={"total": len(model_items), "updated": updated})


def _validate_api_key(request: Request) -> Optional[Response]:
    """
    Returns a Response indicating that the validation did not succeed. Otherwise, None is returned.
    :param request: the incoming request
    :return: a Response in case of a validation error, None otherwise
    """
    expected_api_key = os.environ.get("REST_API_KEY")
    if not expected_api_key:
        logger.warning("Missing REST_API_KEY in config.json")
        return Responder.error(error_message="Configuration mismatch.", status_code=500)

    key_received = request.headers.get("X-API-KEY", "")
    if key_received != expected_api_key:
        logger.warning(
            f"Invalid X-API-KEY header received. Received '{key_received}, expected '{expected_api_key}'")
        return Responder.error(error_message="No permission", status_code=500)

    return None


def _count_items(entity_name: str) -> int:
    items = _db_items(entity_name=entity_name)
    return len(items)


def _db_items(entity_name: str) -> List:
    table_name = os.environ[f"DYNAMO_TABLE_NAME_{entity_name}"]
    logger.info(f"Fetching all items in Dynamo table: {table_name} ..")
    dynamodb_resource = _dynamodb_resource()
    table = dynamodb_resource.Table(table_name)
    response = table.scan()
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        logger.info(f"Scanning next page after {len(items)} results ..")
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))
    return items


def _count_documents(entity_name: str) -> Optional[int]:
    index_name = os.environ[f"INDEX_NAME_{entity_name}"]
    logger.info(f"Counting documents in index: {index_name} ..")

    index_facade = _searchable_index_facade()
    return index_facade.count_documents(index_name=index_name)


def _backfill_profiles(lambda_client: BaseClient) -> int:
    ddb_table_name = os.environ.get("DYNAMO_TABLE_NAME_PROFILES")
    ddb_table_stream_arm = os.environ.get("DYNAMO_TABLE_STREAM_ARN_PROFILES")

    return _run_backfill(
        lambda_client=lambda_client,
        ddb_table_name=ddb_table_name,
        ddb_table_stream_arm=ddb_table_stream_arm
    )


def _backfill_models(lambda_client: BaseClient) -> int:
    ddb_table_name = os.environ.get("DYNAMO_TABLE_NAME_MODELS")
    ddb_table_stream_arm = os.environ.get("DYNAMO_TABLE_STREAM_ARN_MODELS")

    return _run_backfill(
        lambda_client=lambda_client,
        ddb_table_name=ddb_table_name,
        ddb_table_stream_arm=ddb_table_stream_arm
    )


def _run_backfill(lambda_client: BaseClient, ddb_table_name: str, ddb_table_stream_arm: str, limit: int = 300) -> int:
    dynamodb_resource = boto3.resource("dynamodb", region_name=REGION)
    logger.info(f"Using DynamoDB table name: {ddb_table_name}")

    table = dynamodb_resource.Table(ddb_table_name)
    logger.info(f"Found table: {table}")

    ddb_keys_name = [a["AttributeName"] for a in table.key_schema]
    logger.info("Found ddb_keys_name: %s", ddb_keys_name)

    logger.info(f"Using DynamoDB table stream ARN: {ddb_table_stream_arm}")

    lambda_function_name = os.environ.get(
        "DYNAMO_TO_OPENSEARCH_LAMBDA_FUNCTION")
    logger.info(
        f"Using DynamoDB to Opensearch Lambda function "
        f"name: {lambda_function_name}")

    total_reports = 0
    report_batch = []

    response = None
    while True:
        try:
            if not response:
                response = table.scan(Limit=limit)
            else:
                response = table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"], Limit=limit
                )
        except ClientError as e:
            logger.exception(
                f"Error scanning {ddb_table_name}: "
                f"{e.response.get('Error', {}).get('Message')}"
            )
            raise ValueError("Failed to reindex (DynamoDB error).")

        for i in response["Items"]:
            ddb_keys = {k: i[k] for k in i if k in ddb_keys_name}
            ddb_data = boto3.dynamodb.types.TypeSerializer().serialize(i)["M"]
            ddb_keys = boto3.dynamodb.types.TypeSerializer().serialize(ddb_keys)["M"]
            record = {
                "dynamodb": {
                    "SequenceNumber": "0000",
                    "Keys": ddb_keys,
                    "NewImage": ddb_data
                },
                "awsRegion": REGION,
                "eventName": "MODIFY",
                "eventSourceARN": ddb_table_stream_arm,
                "eventSource": "aws:dynamodb"
            }
            total_reports += 1
            report_batch.append(record)

            if len(report_batch) == 100:
                _send_to_elasticsearch_lambda(
                    lambda_client=lambda_client,
                    lambda_function_name=lambda_function_name,
                    reports=report_batch
                )
                logger.info("Resetting report_batch and part_size.")
                report_batch = []

        if "LastEvaluatedKey" not in response:
            logger.info("No additional records to scan ..")
            break

    if len(report_batch) > 0:
        logger.info(f"Sending remaining {len(report_batch)} to Opensearch ..")
        _send_to_elasticsearch_lambda(
            lambda_client=lambda_client,
            lambda_function_name=lambda_function_name,
            reports=report_batch
        )

    return total_reports


def _send_to_elasticsearch_lambda(lambda_client: BaseClient, lambda_function_name: str, reports: List) -> None:
    records_data = {
        "Records": reports
    }
    records = json.dumps(records_data)
    try:
        logger.info(f"About to invoke: {lambda_function_name}")
        lambda_response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            Payload=records
        )
        logger.info(f"Lambda responded with: {lambda_response}")
    except ClientError as e:
        logger.exception(
            f"Error calling {lambda_function_name}: "
            f"{e.response.get('Error', {}).get('Message')}"
        )
        raise ValueError("Failed to reindex (Lambda error).")


# ruff: noqa: ANN202
def _dynamodb_resource():
    dynamodb_port = os.environ.get("DYNAMO_PORT")
    if dynamodb_port:
        return boto3.resource(
            "dynamodb",
            endpoint_url="http://localhost:%s" % (dynamodb_port,),
            region_name=REGION
        )
    else:
        return boto3.resource("dynamodb", region_name=REGION)


def _searchable_index_facade() -> SearchableIndexFacade:
    if FeatureToggles.on(FeatureToggles.STUBBED_OPENSEARCH):
        logger.info("Creating StubbedSearchableIndexFacade under test.")
        return StubbedSearchableIndexFacade.instance()
    else:
        endpoint = os.environ["OPENSEARCH_ENDPOINT"]
        logger.info(f"Creating OpenSearchFacade using: {endpoint}.")
        return OpenSearchFacade(endpoint=endpoint, region=REGION)


def _stripe_facade(secrets: Secrets) -> StripeFacade:
    if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
        logger.info("Creating StubbedStripeFacade under test.")
        return StubbedStripeFacade.instance()
    else:
        logger.info("Creating StripeFacade.")
        return StripeFacade(secrets=secrets)


def _sitemap_storage_facade() -> SitemapStorageFacade:
    if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
        logger.info("Creating StubbedSitemapStorageFacade under test.")
        return StubbedSitemapStorageFacade.instance()
    else:
        logger.info("Creating AmazonS3SitemapStorageFacade.")
        client = boto3.client("s3")
        bucket = os.environ["BUCKET_SITEMAPS"]
        return AmazonS3SitemapStorageFacade(s3_client=client, bucket_name=bucket)


def _cloud_utils(secrets: Optional[Secrets]) -> CloudUtils:
    if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
        logger.info("Creating StubbedCloudUtils under test.")
        return StubbedCloudUtils.instance()
    else:
        logger.info("Creating CloudUtils.")
        endpoint = os.environ["GRAPHQL_API_ENDPOINT"]
        ses_client = boto3.client("ses", region_name=REGION)
        cognito_identity_client = boto3.client("cognito-idp", region_name=REGION)
        return CloudUtils(app_sync_url=f"https://{endpoint}/graphql", secrets=secrets, ses_client=ses_client,
                          cognito_identity_client=cognito_identity_client)


def _secrets() -> Secrets:
    global SECRETS
    if SECRETS is None:
        if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
            logger.info(
                "Creating SECRETS from env variable 'SECRETS_JSON' under test"
            )
            SECRETS = Secrets.from_json(os.environ["SECRETS_JSON"])
        else:
            client = boto3.client("ssm")
            app_name_parts = app.app_name.split("-")
            app_name_parts.append(STAGE)
            key_name = f'/aws/reference/secretsmanager/{"-".join(app_name_parts)}'
            logger.info(f"Creating Secrets from SSM parameter named '{key_name}'")

            try:
                parameter = client.get_parameter(Name=key_name, WithDecryption=True)
                secrets_json = parameter.get("Parameter", {}).get("Value")
                SECRETS = Secrets.from_json(secrets_json)
            except ClientError as e:
                logger.exception(
                    "Unable to get parameter: %s. %s"
                    % (key_name, e.response.get("Error", {}).get("Message"))
                )
                raise ValueError(f"Unable to fetch parameter {key_name} from SSM.")

            gcp_credentials = _gcp_credentials()
            SECRETS.gcp_credentials = gcp_credentials

    return SECRETS


def _gcp_credentials() -> GcpCredentials:
    if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
        logger.info(
            "Creating GcpCredentials under test"
        )
        return GcpCredentials(
            type="service_account", project_id="project_id", private_key_id="private_key_id", private_key="private_key",
            client_email="client_email", client_id="client_id", auth_uri="auth_uri", token_uri="token_uri",
            auth_provider_x509_cert_url="auth_provider_x509_cert_url", client_x509_cert_url="client_x509_cert_url"
        )
    else:
        client = boto3.client("ssm")
        app_name_parts = app.app_name.split("-")
        app_name_parts.append(STAGE)
        app_name_parts.append("service")
        app_name_parts.append("account")
        key_name = f'/aws/reference/secretsmanager/{"-".join(app_name_parts)}'
        logger.info(f"Creating GcpCredentials from SSM parameter named '{key_name}'")

        try:
            parameter = client.get_parameter(Name=key_name, WithDecryption=True)
            credentials_json = parameter.get("Parameter", {}).get("Value")
            return GcpCredentials.from_json(credentials_json)
        except ClientError as e:
            logger.exception(
                "Unable to get parameter: %s. %s"
                % (key_name, e.response.get("Error", {}).get("Message"))
            )
            raise ValueError(f"Unable to fetch parameter {key_name} from SSM.")


def _database_facade() -> DatabaseFacade:
    global DATABASE_FACADE
    if DATABASE_FACADE is None:
        if FeatureToggles.on(FeatureToggles.TEST_CONTEXT):
            logger.info("Creating MemoryDatabaseFacade under test")
            DATABASE_FACADE = MemoryDatabaseFacade.instance()
        else:
            logger.info("Using DynamoDBFacade in non-test context.")
            client = boto3.client("dynamodb")
            table_name = os.environ["DYNAMO_TABLE_NAME_UTILS"]
            DATABASE_FACADE = DynamoDBFacade(table_name=table_name, client=client)

    return DATABASE_FACADE
