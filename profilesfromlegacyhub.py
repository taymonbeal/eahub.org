#!/usr/bin/env python

# pip install Django django-environ mysqlclient phpserialize

import contextlib
import datetime
import itertools
import json

import MySQLdb
from django.core.serializers import json as django_json
import environ
import phpserialize


db_config = environ.Env().db_url("LEGACY_DATABASE_URL")
with contextlib.closing(
    MySQLdb.connect(
        host=db_config["HOST"],
        user=db_config["USER"],
        passwd=db_config["PASSWORD"],
        db=db_config["NAME"],
        port=int(db_config["PORT"] or 3306),
    )
) as connection, connection.cursor() as cursor:
    cursor.execute("SELECT DISTINCT type FROM profile ORDER BY type;")
    profile_types = [profile_type for profile_type, in cursor.fetchall()]
    cursor.execute("SELECT DISTINCT type FROM node ORDER BY type;")
    node_types = [node_type for node_type, in cursor.fetchall()]
    cursor.execute(
        "SELECT DISTINCT type, TRIM(LEADING 'eform_' FROM type) "
        "FROM entityform "
        "ORDER BY type;"
    )
    entityform_pretty_names = dict(cursor.fetchall())
    cursor.execute(
        "SELECT "
        "users.uid, users.name, users.mail, FROM_UNIXTIME(users.created), "
        "IF(users.login, FROM_UNIXTIME(users.login), NULL), users.status, "
        "TRIM(LEADING 'public://' FROM file_managed.uri), "
        "TRIM(LEADING 'user/' FROM url_alias.alias) "
        "FROM users "
        "LEFT JOIN file_managed "
        "ON users.picture = file_managed.fid AND file_managed.uri LIKE 'public://%' "
        "LEFT JOIN url_alias ON CONCAT('user/', users.uid) = url_alias.source "
        "ORDER BY users.mail;"
    )
    users = {
        uid: {
            "name": name,
            "mail": mail,
            "created": created,
            "login": login,
            "status": bool(status),
            "picture": picture,
            "alias": alias,
            "authnames": [],
            "redirects": [],
            **{profile_type: [] for profile_type in profile_types},
            **{node_type: [] for node_type in node_types},
            **{pretty_name: [] for pretty_name in entityform_pretty_names.values()},
        }
        for uid, name, mail, created, login, status, picture, alias in cursor.fetchall()
    }
    cursor.execute("SELECT uid, authname FROM authmap ORDER BY uid, authname;")
    for uid, rows in itertools.groupby(cursor.fetchall(), key=lambda row: row[0]):
        users[uid]["authnames"] = [authname for _, authname in rows]
    cursor.execute(
        "SELECT DISTINCT "
        "CAST(TRIM(LEADING 'user/' FROM redirect) AS UNSIGNED) AS uid, "
        "TRIM(LEADING 'user/' FROM TRIM(LEADING 'users/' FROM source)) AS path "
        "FROM redirect "
        "WHERE redirect LIKE 'user/%' AND status "
        "ORDER BY uid, path;"
    )
    for uid, rows in itertools.groupby(cursor.fetchall(), key=lambda row: row[0]):
        users[uid]["redirects"] = [path for _, path in rows]
    cursor.execute(
        "SELECT "
        "type, field_name, cardinality < 0, TRIM(LEADING 'field_' FROM field_name) "
        "FROM field_config "
        "WHERE type NOT IN ('datetime', 'file') "
        "ORDER BY type;"
    )
    field_rows = cursor.fetchall()
    fields_by_data_type = {
        data_type: [field for _, field, _, _ in rows]
        for data_type, rows in itertools.groupby(field_rows, key=lambda row: row[0])
    }
    plural_fields = {field for _, field, is_plural, _ in field_rows if is_plural}
    field_pretty_names = {field: pretty_name for _, field, _, pretty_name in field_rows}
    cursor.execute(
        " UNION ALL ".join(
            "("
            f"SELECT DISTINCT entity_type, bundle, '{field}' AS field "
            f"FROM field_data_{field}"
            ")"
            for field in field_pretty_names
        )
        + " ORDER BY entity_type, bundle, field;"
    )
    fields_by_bundle = {
        entity_type: {
            bundle: [field for _, _, field in rows]
            for bundle, rows in itertools.groupby(
                rows_by_entity_type, key=lambda row: row[1]
            )
        }
        for entity_type, rows_by_entity_type in itertools.groupby(
            cursor.fetchall(), key=lambda row: row[0]
        )
    }
    profiles = {}
    cursor.execute(
        "SELECT "
        "IFNULL(uid, 0) AS uid, type, pid, FROM_UNIXTIME(created), "
        "FROM_UNIXTIME(changed) "
        "FROM profile "
        "ORDER BY uid, type, created, pid;"
    )
    for uid, rows_by_uid in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        user = users[uid]
        for profile_type, rows in itertools.groupby(
            rows_by_uid, key=lambda row: row[1]
        ):
            profiles_for_user = user[profile_type]
            for _, _, pid, created, changed in rows:
                profile = {"created": created, "changed": changed}
                if profile_type in fields_by_bundle["profile2"]:
                    profile.update(
                        {
                            field_pretty_names[field]: (
                                [] if field in plural_fields else None
                            )
                            for field in fields_by_bundle["profile2"][profile_type]
                        }
                    )
                profiles[pid] = profile
                profiles_for_user.append(profile)
    nodes = {}
    cursor.execute(
        "SELECT "
        "uid, type, nid, title, FROM_UNIXTIME(created), FROM_UNIXTIME(changed) "
        "FROM node "
        "ORDER BY uid, type, created, title, nid;"
    )
    for uid, rows_by_uid in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        user = users[uid]
        for node_type, rows in itertools.groupby(rows_by_uid, key=lambda row: row[1]):
            nodes_for_user = user[node_type]
            for _, _, nid, title, created, changed in rows:
                node = {"title": title, "created": created, "changed": changed}
                if node_type in fields_by_bundle["node"]:
                    node.update(
                        {
                            field_pretty_names[field]: (
                                [] if field in plural_fields else None
                            )
                            for field in fields_by_bundle["node"][node_type]
                        }
                    )
                nodes[nid] = node
                nodes_for_user.append(node)
    entityforms = {}
    cursor.execute(
        "SELECT "
        "uid, type, entityform_id, FROM_UNIXTIME(created), FROM_UNIXTIME(changed) "
        "FROM entityform "
        "ORDER BY uid, type, created, entityform_id;"
    )
    for uid, rows_by_uid in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        user = users[uid]
        for entityform_type, rows in itertools.groupby(
            rows_by_uid, key=lambda row: row[1]
        ):
            entityforms_for_user = user[entityform_pretty_names[entityform_type]]
            for _, _, entityform_id, created, changed in rows:
                entityform = {"created": created, "changed": changed}
                if entityform_type in fields_by_bundle["entityform"]:
                    entityform.update(
                        {
                            field_pretty_names[field]: (
                                [] if field in plural_fields else None
                            )
                            for field in fields_by_bundle["entityform"][entityform_type]
                        }
                    )
                entityforms[entityform_id] = entityform
                entityforms_for_user.append(entityform)
    field_collection_items = {}
    cursor.execute(
        "SELECT field_name, item_id, archived "
        "FROM field_collection_item "
        "ORDER BY field_name, item_id;"
    )
    for field_name, rows in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        for _, item_id, archived in rows:
            field_collection_item = {"archived": bool(archived)}
            if field_name in fields_by_bundle["field_collection_item"]:
                field_collection_item.update(
                    {
                        field_pretty_names[field]: (
                            [] if field in plural_fields else None
                        )
                        for field in fields_by_bundle["field_collection_item"][
                            field_name
                        ]
                    }
                )
            field_collection_items[item_id] = field_collection_item
    entities_by_type = {
        "profile2": profiles,
        "node": nodes,
        "entityform": entityforms,
        "field_collection_item": field_collection_items,
    }
    cursor.execute(
        " UNION ALL ".join(
            [
                "("
                "SELECT "
                f"entity_type, entity_id, '{field}' AS field, {field}_value, delta "
                f"FROM field_data_{field}"
                ")"
                for field in fields_by_data_type["number_integer"]
            ]
            + [
                "("
                "SELECT "
                f"entity_type, entity_id, '{field}' AS field, {field}_value DIV 1, "
                "delta "
                f"FROM field_data_{field}"
                ")"
                for field in fields_by_data_type["number_decimal"]
            ]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [value for _, _, _, value, _ in rows]
                else:
                    (_, _, _, entity[pretty_name], _), = rows
    cursor.execute(
        " UNION ALL ".join(
            "("
            "SELECT "
            f"entity_type, entity_id, '{field}' AS field, {field}_value, "
            f"{field}_format, delta "
            f"FROM field_data_{field}"
            ")"
            for field in (
                fields_by_data_type["text"]
                + fields_by_data_type["text_long"]
                + fields_by_data_type["text_with_summary"]
            )
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [
                        {"value": value, "format": fmt}
                        for _, _, _, value, fmt, _ in rows
                    ]
                else:
                    (_, _, _, value, fmt, _), = rows
                    entity[pretty_name] = {"value": value, "format": fmt}
    cursor.execute(
        " UNION ALL ".join(
            [
                "("
                "SELECT "
                f"entity_type, entity_id, '{field}' AS field, {field}_url, {field}_title, "
                "delta "
                f"FROM field_data_{field}"
                ")"
                for field in fields_by_data_type["link_field"]
            ]
            + [
                "("
                "SELECT "
                f"entity_type, entity_id, '{field}' AS field, {field}_value, {field}_title, "
                "delta "
                f"FROM field_data_{field}"
                ")"
                for field in fields_by_data_type["url"]
            ]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [
                        {"url": url, "title": title} for _, _, _, url, title, _ in rows
                    ]
                else:
                    (_, _, _, url, title, _), = rows
                    entity[pretty_name] = {"url": url, "title": title}
    cursor.execute(
        " UNION ALL ".join(
            [
                "("
                "SELECT "
                f"entity_type, entity_id, '{field}' AS field, {field}_value, delta "
                f"FROM field_data_{field}"
                ")"
                for field in fields_by_data_type["list_boolean"]
            ]
            + [
                "("
                "SELECT "
                f"entity_type, entity_id, '{field}' AS field, {field}_value = 'Yes', "
                "delta "
                f"FROM field_data_{field}"
                ")"
                for field in fields_by_data_type["list_text"]
            ]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [bool(value) for _, _, _, value, _ in rows]
                else:
                    (_, _, _, value, _), = rows
                    entity[pretty_name] = bool(value)
    cursor.execute(
        " UNION ALL ".join(
            "("
            "SELECT "
            f"entity_type, entity_id, '{field}' AS field, DATE({field}_value), "
            f"DATE({field}_value2), delta "
            f"FROM field_data_{field}"
            ")"
            for field in fields_by_data_type["date"]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [
                        {"start": start, "end": end} for _, _, _, start, end, _ in rows
                    ]
                else:
                    (_, _, _, start, end, _), = rows
                    entity[pretty_name] = {"start": start, "end": end}
    cursor.execute(
        " UNION ALL ".join(
            "("
            f"SELECT entity_type, entity_id, '{field}' AS field, {field}_value, delta "
            f"FROM field_data_{field}"
            ")"
            for field in fields_by_data_type["field_collection"]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [
                        field_collection_items[value] for _, _, _, value, _ in rows
                    ]
                else:
                    (_, _, _, value, _), = rows
                    entity[pretty_name] = field_collection_items[value]
    cursor.execute(
        " UNION ALL ".join(
            "("
            f"SELECT field_data_{field}.entity_type, field_data_{field}.entity_id, "
            f"'{field}' AS field, TRIM(LEADING 'public://' FROM file_managed.uri), "
            f"field_data_{field}.{field}_width, field_data_{field}.{field}_height, "
            f"field_data_{field}.delta "
            f"FROM field_data_{field} "
            "LEFT JOIN file_managed "
            f"ON field_data_{field}.{field}_fid = file_managed.fid "
            "AND file_managed.uri LIKE 'public://%'"
            ")"
            for field in fields_by_data_type["image"]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [
                        {"file": uri, "width": width, "height": height}
                        for _, _, _, uri, width, height, _ in rows
                    ]
                else:
                    (_, _, _, uri, width, height, _), = rows
                    entity[pretty_name] = {
                        "file": uri,
                        "width": width,
                        "height": height,
                    }
    cursor.execute(
        " UNION ALL ".join(
            "("
            "SELECT "
            f"entity_type, entity_id, '{field}' AS field, "
            f"CAST({field}_value AS BINARY), delta "
            f"FROM field_data_{field} "
            ")"
            for field in fields_by_data_type["tablefield"]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    for _, _, _, value, _ in rows:
                        if value is None:
                            entity[pretty_name].append(None)
                            continue
                        table = phpserialize.loads(value, decode_strings=True)
                        entity[pretty_name].append(
                            [
                                {
                                    table[f"cell_0_{table_column}"]: table[
                                        f"cell_{table_row}_{table_column}"
                                    ]
                                    for table_column in range(
                                        int(table["rebuild"]["count_cols"])
                                    )
                                }
                                for table_row in range(
                                    1, int(table["rebuild"]["count_rows"])
                                )
                            ]
                        )
                else:
                    (_, _, _, value, _), = rows
                    if value is None:
                        continue
                    table = phpserialize.loads(value, decode_strings=True)
                    entity[pretty_name] = [
                        {
                            table[f"cell_0_{table_column}"]: table[
                                f"cell_{table_row}_{table_column}"
                            ]
                            for table_column in range(
                                int(table["rebuild"]["count_cols"])
                            )
                        }
                        for table_row in range(1, int(table["rebuild"]["count_rows"]))
                    ]
    cursor.execute(
        " UNION ALL ".join(
            "("
            f"SELECT field_data_{field}.entity_type, field_data_{field}.entity_id, "
            f"'{field}' AS field, taxonomy_term_data.name, field_data_{field}.delta "
            f"FROM field_data_{field} "
            "LEFT JOIN taxonomy_term_data "
            f"ON field_data_{field}.{field}_tid = taxonomy_term_data.tid"
            ")"
            for field in fields_by_data_type["taxonomy_term_reference"]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:
                    entity[pretty_name] = [value for _, _, _, value, _ in rows]
                else:
                    (_, _, _, entity[pretty_name], _), = rows
    cursor.execute(
        " UNION ALL ".join(
            "("
            f"SELECT field_data_{field}.entity_type, field_data_{field}.entity_id, "
            f"'{field}' AS field, users.name, users.mail, field_data_{field}.delta "
            f"FROM field_data_{field} "
            f"LEFT JOIN users ON field_data_{field}.{field}_uid = users.uid"
            ")"
            for field in fields_by_data_type["user_reference"]
        )
        + " ORDER BY entity_type, entity_id, field, delta;"
    )
    for entity_type, rows_by_entity_type in itertools.groupby(
        cursor.fetchall(), key=lambda row: row[0]
    ):
        entities = entities_by_type[entity_type]
        for entity_id, rows_by_entity_id in itertools.groupby(
            rows_by_entity_type, key=lambda row: row[1]
        ):
            entity = entities.setdefault(entity_id, {})
            for field, rows in itertools.groupby(
                rows_by_entity_id, key=lambda row: row[2]
            ):
                pretty_name = field_pretty_names[field]
                if field in plural_fields:

                    entity[pretty_name] = [
                        {"name": name, "mail": mail} for _, _, _, name, mail, _ in rows
                    ]
                else:
                    (_, _, _, name, mail, _), = rows
                    entity[pretty_name] = {"name": name, "mail": mail}
print(
    json.dumps(
        sorted(users.values(), key=lambda user: user["name"]),
        cls=django_json.DjangoJSONEncoder,
        indent=2,
    )
)
