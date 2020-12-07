from mongoengine import IntField, FloatField, StringField, BooleanField, Document, EmbeddedDocument, \
    ReferenceField, \
    DateField, EmbeddedDocumentField


class RegionInfo(Document):
    code = IntField()
    province = StringField()
    city = StringField()
    elementary_school_count = IntField()
    kindergarten_count = IntField()
    university_count = IntField()
    nursing_home_count = IntField()
    academy_ratio = FloatField()
    elderly_population_ratio = FloatField()
    elderly_alone_ratio = FloatField()


class Region(EmbeddedDocument):
    code = IntField()
    province = StringField()
    city = StringField()
    elementary_school_count = IntField()
    kindergarten_count = IntField()
    university_count = IntField()
    nursing_home_count = IntField()


class Case(Document):
    case_id = IntField()
    group = BooleanField()
    infection_case = StringField()
    confirmed = IntField()
    region = ReferenceField(RegionInfo)
    meta = {
        'indexes': [
            '$infection_case',  # text index
        ]
    }


class DateHistory(EmbeddedDocument):
    symptom_onset_date = DateField()
    confirmed_date = DateField()
    released_date = DateField()
    deceased_date = DateField()


class PatientInfo(Document):
    patient_id = StringField()
    sex = StringField()
    age = IntField()
    region = (EmbeddedDocumentField(Region))
    infection_case = StringField()
    infected_by = StringField()
    contact_number = IntField()
    date_history = (EmbeddedDocumentField(DateHistory))
    state = StringField()
    meta = {
        'indexes': [
            '$infection_case',  # text index
        ]
    }
