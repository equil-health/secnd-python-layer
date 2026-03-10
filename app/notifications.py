"""FCM push notification sender for Breaking pipeline.

Uses firebase-admin SDK. Requires FIREBASE_SERVICE_ACCOUNT_PATH in .env.
"""

import logging

from .config import settings

logger = logging.getLogger(__name__)

_firebase_app = None


def _get_firebase_app():
    """Lazy-initialize Firebase app."""
    global _firebase_app
    if _firebase_app is not None:
        return _firebase_app

    if not settings.FIREBASE_SERVICE_ACCOUNT_PATH:
        logger.warning("FIREBASE_SERVICE_ACCOUNT_PATH not set — push disabled")
        return None

    try:
        import firebase_admin
        from firebase_admin import credentials

        cred = credentials.Certificate(settings.FIREBASE_SERVICE_ACCOUNT_PATH)
        _firebase_app = firebase_admin.initialize_app(cred)
        logger.info("Firebase app initialized for push notifications")
        return _firebase_app
    except Exception as e:
        logger.error(f"Firebase init failed: {e}")
        return None


def send_breaking_notifications(all_headlines: dict[str, list[dict]]):
    """Send FCM notifications after daily Breaking refresh.

    ALERTs → high priority, immediate delivery.
    No ALERTs → normal priority daily digest.
    """
    fb_app = _get_firebase_app()
    if not fb_app:
        return

    from firebase_admin import messaging
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from .models.breaking import DoctorPreferences

    sync_url = settings.DATABASE_URL.replace("+asyncpg", "+psycopg2")
    engine = create_engine(sync_url)
    Session = sessionmaker(bind=engine)

    # Collect ALERTs
    alerts = []
    for sp, headlines in all_headlines.items():
        for h in headlines:
            if h.get("urgency_tier") == "ALERT":
                alerts.append(h)

    has_alerts = len(alerts) > 0

    # Get all doctors with push tokens
    with Session() as db:
        doctors = (
            db.query(DoctorPreferences)
            .filter(
                DoctorPreferences.push_token.isnot(None),
                DoctorPreferences.breaking_enabled == True,  # noqa: E712
            )
            .all()
        )

    if not doctors:
        logger.info("[Push] No doctors with push tokens")
        return

    tokens = [d.push_token for d in doctors if d.push_token]
    if not tokens:
        return

    # Build notification
    if has_alerts:
        title = f"SECND Pulse: {len(alerts)} ALERT{'s' if len(alerts) > 1 else ''}"
        body = alerts[0].get("title", "New medical alert")[:100]
        priority = "high"
    else:
        total = sum(len(v) for v in all_headlines.values())
        title = "SECND Pulse: Daily Digest"
        body = f"{total} new medical headlines across your specialties"
        priority = "normal"

    # Send in batches of 500 (FCM multicast limit)
    total_success = 0
    total_failure = 0

    for i in range(0, len(tokens), 500):
        batch_tokens = tokens[i:i + 500]
        message = messaging.MulticastMessage(
            tokens=batch_tokens,
            notification=messaging.Notification(title=title, body=body),
            android=messaging.AndroidConfig(
                priority=priority,
                notification=messaging.AndroidNotification(
                    channel_id="breaking_alerts" if has_alerts else "breaking_daily",
                ),
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        badge=len(alerts) if has_alerts else 0,
                        sound="default",
                    ),
                ),
            ),
            data={
                "type": "breaking",
                "alert_count": str(len(alerts)),
            },
        )

        try:
            response = messaging.send_each_for_multicast(message)
            total_success += response.success_count
            total_failure += response.failure_count
        except Exception as e:
            logger.error(f"[Push] FCM batch send failed: {e}")
            total_failure += len(batch_tokens)

    logger.info(
        f"[Push] Sent to {len(tokens)} tokens: "
        f"{total_success} success, {total_failure} failure"
    )
