import datetime

# How long a user has before he can no longer delete his neighbor message.
DELETION_WINDOW = datetime.timedelta(minutes=10)
EDIT_WINDOW = datetime.timedelta(minutes=15)

# Allowed values for the status field.
UNREVIEWED = 0
LIVE_BAD = 5
LIVE_GOOD = 1
LIVE_MEDIOCRE = 2
REMOVED_BY_STAFF = 3
REMOVED_BY_USER = 4
AWAITING_PAYMENT = 8
AWAITING_STAFF_APPROVAL = 9
REJECTED = 10
UNDER_REVIEW = 11
DELETED = 12
REMOVED_BY_SPAM_FILTER = 14

STATUS_LIVE = (UNREVIEWED, LIVE_BAD, LIVE_GOOD, LIVE_MEDIOCRE)
STATUS_PENDING = (AWAITING_PAYMENT, AWAITING_STAFF_APPROVAL, UNDER_REVIEW)
STATUS_REMOVED = (REMOVED_BY_STAFF, REMOVED_BY_USER, REJECTED, DELETED, REMOVED_BY_SPAM_FILTER)

STATUS_CHOICES = [
    (UNREVIEWED, 'Live/unreviewed'),
    (LIVE_GOOD, 'Live/good'),
    (LIVE_BAD, 'Live/bad'),
    (LIVE_MEDIOCRE, 'Live/mediocre'),
    (REMOVED_BY_STAFF, 'Removed by staff'),
    (REMOVED_BY_USER, 'Removed by user'),
    (REMOVED_BY_SPAM_FILTER, 'Remove by spam filter'),
    (DELETED, 'Deleted'),
    (AWAITING_PAYMENT, 'Awaiting payment'),
    (AWAITING_STAFF_APPROVAL, 'Awaiting staff approval'),
    (REJECTED, 'Rejected'),
    (UNDER_REVIEW, 'Under review'),
]

AD_STATUSES = [
    (LIVE_GOOD, 'Live/good'),
    (AWAITING_PAYMENT, 'Awaiting payment'),
    (AWAITING_STAFF_APPROVAL, 'Awaiting staff approval'),
    (UNDER_REVIEW, 'Under review'),
    (REJECTED, 'Rejected'),
    (REMOVED_BY_USER, 'Removed by user'),
]

POST_CATEGORIES = [
    ['kindness', 'acts of kindness', 'Neighbors doing good deeds'],
    ['city-services', 'city services', 'Questions or announcements related to trash pickup, utilities, etc.'],
    ['crime-posts', 'crime', 'Recent crimes or suspicious activity'],
    ['family', 'family/parenting', 'Daycare, kids\' classes, playgroups, etc.'],
    ['housing', 'housing', 'Apartment searches, home maintenance, local real estate trends, etc.'],
    ['local-business', 'local businesses', 'Business reviews and requests for recommendations'],
    ['politics', 'local politics', 'Local elections and announcements. No personal attacks, please'],
    ['lost-found', 'lost and found', 'Found keys, lost cameras, etc.'],
    ['improvement', 'neighborhood improvement', 'Projects to make your neighborhood a better place'],
    ['announcements', 'neighborhood news and talk ', 'Discussions, announcements and questions about things happening nearby'],
    ['pets', 'pets', 'Lost pets, found pets, dog parks, and alerts for local pet owners'],
    ['schools', 'schools','Questions, thoughts and recommendations for schools, busing, tutoring, etc.'],
]


class RemovalReasons(object):
    # Legacy reasons
    DELETE = 0
    OFF_TOPIC = 1
    RANT = 4
    OFFENSIVE = 5
    COMMERCIAL = 6
    AD_HOMINEM = 7
    # Current reasons
    HARMFUL = 9
    INAPPROPRIATE = 10
    PERSONAL_ATTACK = 11
    SPAM = 12
    REPEATEDLY_FLAGGED = 13
    ADVERTISEMENT = 14
    LIBEL = 8
    PRIVACY = 2
    COPYRIGHT = 3
    DUPLICATE = 15

    LEGACY_REASONS = (
        (AD_HOMINEM, "Ad hominem"),
        (COMMERCIAL, "Commercial"),
        (OFF_TOPIC, "Off-topic"),
        (OFFENSIVE, "Offensive"),
        (RANT, "Rant"),
    )

    CURRENT_REASONS = (
        (HARMFUL, 'Harmful content'),
        (INAPPROPRIATE, 'Inappropriate content'),
        (PERSONAL_ATTACK, 'Personal attack'),
        (SPAM, 'Spam'),
        (REPEATEDLY_FLAGGED, 'Repeatedly flagged'),
        (ADVERTISEMENT, 'Advertisement'),
        (LIBEL, 'Libel'),
        (PRIVACY, 'Privacy violation'),
        (COPYRIGHT, 'Copyright violation'),
        (DUPLICATE, 'Duplicate'),
    )

    REASONS = CURRENT_REASONS + LEGACY_REASONS + ((DELETE, "DELETE"),)

    DETAILS = {
        HARMFUL: {
            'text': 'Harmful content',
            'slug': 'harmful-content',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'contains harmful content.'
            ),
        },
        INAPPROPRIATE: {
            'text': 'Inappropriate content',
            'slug': 'inappropriate-content',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'contains inappropriate content.'
            ),
        },
        PERSONAL_ATTACK: {
            'text': 'Personal attack',
            'slug': 'personal-attack',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'is a personal attack.'
            ),
        },
        SPAM: {
            'text': 'Spam',
            'slug': 'spam',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'is considered spam.'
            ),
        },
        REPEATEDLY_FLAGGED: {
            'text': 'Repeatedly flagged',
            'slug': 'repeatedly-flagged',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'has been repeatedly reported.'
            ),
        },
        ADVERTISEMENT: {
            'text': 'Advertisement',
            'slug': 'advertisement',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'is considered an advertisement.'
            ),
        },
        LIBEL: {
            'text': 'Libel',
            'slug': 'libel',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'is considered to be libelous.'
            ),
        },
        PRIVACY: {
            'text': 'Privacy violation',
            'slug': 'privacy',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'violates privacy.'
            ),
        },
        COPYRIGHT: {
            'text': 'Copyright violation',
            'slug': 'copyright',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'violates copyright.'
            ),
        },
        DUPLICATE: {
            'text': 'Duplicate message',
            'short_message': (
                'This {} has been removed by EveryBlock staff because it '
                'is a duplicate.'
            ),
        },
    }

# All user-contributed schema slugs.
NEIGHBOR_MESSAGE_SCHEMAS = [c[0] for c in POST_CATEGORIES]
NEIGHBOR_MESSAGE_CHOICES = [(c[0], c[1][0].upper() + c[1][1:]) for c in POST_CATEGORIES]
USER_SCHEMAS = ['neighbor-events'] + NEIGHBOR_MESSAGE_SCHEMAS
UGC = ['neighbor-ads'] + USER_SCHEMAS # All user-generated content
