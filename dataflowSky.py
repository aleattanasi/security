import argparse
import datetime
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window


class GroupWindowsIntoBatches(beam.PTransform):


    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size):
        self.window_size = int(window_size * 3)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its
            # publish timestamp.
            | "Window into Fixed Intervals"
            >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
            # Use a dummy key to group the elements in the same window.
            # Note that all the elements in one window must fit into memory
            # for this. If the windowed elements do not fit into memory,
            # please consider using `beam.util.BatchElements`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()

            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )


class AddTimestamps(beam.DoFn):


    def process(self, element, publish_time=beam.DoFn.TimestampParam,save_main_session=True):


        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """

        yield {
            "message_body": element.decode("utf-8"),
         #   "publish_time": datetime.datetime.now().isoformat(),
        }


class WriteBatchesToGCS(beam.DoFn):


    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch, window=beam.DoFn.WindowParam,save_main_session=True):
        import os
        import firebase_admin
        from firebase_admin import credentials
        from google.cloud import firestore
        from firebase_admin import firestore

        """Write one batch per file to a Google Cloud Storage bucket. """

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        filename = "-".join([self.output_path, window_start, window_end])

        ta= "TA.json"
        tafname = "-".join([window_start, window_end, ta])
        tm = "TM.json"
        tmfname = "-".join([window_start, window_end, tm])
        dh = "DH.json"
        dhfname = "-".join([window_start, window_end, dh])

        #eventsList = sorted(elements, key=lambda k:k['message_body']["timestamp"])
        elements = []
        for element in batch:
            # f.write("{}\n".format(json.dumps(element)).encode("utf-8"))
            # ELEMENT è IL MESSAGGIO CHE PROVIENE DALLA PUBSUB. ora costruisco una lista di dizionari(messaggi) e poi ne faccio l'elaborazione(payload)
            b = element['message_body']
            c = json.loads(b)
            elements.append(c)
            eventsList = sorted(elements, key=lambda k: k["properties"]["event_timestamp"])



            TV_APPS = []
            TELEMETRIA = []
            DIGITAL_HUB = []
        def sessionHandler(nomeEvento, evento):
            from google.cloud import firestore

            database = firestore.Client()
            if (nomeEvento == 'SESSION_BEGIN'):
                client_id =evento["properties"]["client_id"]
                doc_ref = database.collection(u'sessions').document(evento["properties"]["session_id"])
                doc_ref.set({
                        u'context_info': {
                            u'msg_ts': evento["properties"]["event_timestamp"],
                            u'source': evento["properties"]["source"],
                            u'dev_family': evento["properties"]["dev_family"],
                            u'dev_type': evento["properties"]["dev_type"],
                            u'dev_stb_sn': evento["properties"]["dev_stb_sn"],
                            u'dev_stb_id': evento["properties"]["dev_stb_id"],
                            u'dev_stb_ver': evento["properties"]["dev_stb_ver"],
                            u'dev_stb_model': evento["properties"]["dev_stb_model"],
                            u'dev_stb_name': evento["properties"]["dev_stb_name"],
                            u'dev_stb_man': evento["properties"]["dev_stb_man"],
                            u'dev_stb_as': evento["properties"]["dev_stb_as"],
                            u'dev_stb_ua': evento["properties"]["dev_stb_ua"],
                            u'dev_sc_sn': evento["properties"]["dev_sc_sn"],
                            u'conn_type': evento["properties"]["conn_type"],
                            u'conn_wifi_freq': evento["properties"]["conn_wifi_freq"],
                            u'conn_wifi_channel': evento["properties"]["conn_wifi_channel"],
                            u'conn_wifi_strength': evento["properties"]["conn_wifi_strength"],
                            u'conn_router_model': evento["properties"]["conn_router_model"],
                            u'user_external_id': evento["properties"]["user_external_id"],
                            u'user_country_code': evento["properties"]["user_country_code"],
                            u'user_region_name': evento["properties"]["user_region_name"],
                            u'user_province': evento["properties"]["user_province"],
                            u'user_city': evento["properties"]["user_city"],
                            u'user_isp_name': evento["properties"]["user_isp_name"],
                            u'app_name': evento["properties"]["app_name"],
                            u'app_version': evento["properties"]["app_version"],
                            u'app_itv_be_version': evento["properties"]["app_itv_be_version"],
                        },
                }, merge=True)

            elif (nomeEvento== 'SESSION_END'):
                doc_ref = database.collection(u'sessions').document(evento["properties"]["session_id"]).delete()

        def getCollections(session_id):
            from google.cloud import firestore

            database = firestore.Client()
            a = database.collection(u'sessions').document(session_id).get()
            b = database.collection(u'sessions').document().get()

            docs = database.collection(u'sessions').stream()
            dati = a._data["context_info"]  # questo è un dizionario
            return dati

        def DL_TM_APP_ENTRY(context_info, evento):
            from google.cloud import firestore

            context_info["event_ts"] = evento["properties"]["event_timestamp"]
            context_info["event_name"] = "APP_ENTRY"
            context_info["event_p01"] = evento["properties"]["session_id"]
            context_info["event_p02"] = evento["info"]["app_code"]
            context_info["event_p03"] = evento["properties"]["app_name"]
            context_info["event_p04"] = ""
            context_info["event_p05"] = ""
            context_info["event_p06"] = ""
            context_info["event_p07"] = ""
            context_info["event_p08"] = ""
            context_info["event_p09"] = ""
            context_info["event_p10"] = ""
            context_info["event_p11"] = ""
            context_info["event_p12"] = ""
            context_info["event_p13"] = ""
            context_info["event_p14"] = ""
            context_info["event_p15"] = ""
            messaggio = {"event_id": "ID-XXX", "schema_version":"V-XXXX", "ts":evento["properties"]["event_timestamp"], "entity_name": "TM" , "payload":context_info}

            database = firestore.Client()
            doc_ref = database.collection(u'sessions').document(evento["properties"]["session_id"])
            appEntryTimestamp = evento["properties"]["event_timestamp"]
            doc_ref.set({
                u"app_entry_timestamp": appEntryTimestamp,
            }, merge=True)
            TELEMETRIA.append(messaggio)

        def DL_TM_APP_STARTUP_TIME(context_info, evento, scr_loaded_ts, save_main_session=True):
            from google.cloud import firestore

            database = firestore.Client()
            a = database.collection(u'sessions').document(session_id).get()
            app_entry_ts = a._data["app_entry_timestamp"]
            screen_loaded_timestamp = datetime.datetime.strptime(scr_loaded_ts, "%Y-%m-%dT%H:%M:%SZ")
            app_entry_timestamp = datetime.datetime.strptime(app_entry_ts, "%Y-%m-%dT%H:%M:%SZ")


            startup_time_ms = (screen_loaded_timestamp-app_entry_timestamp)/1000
            context_info["event_ts"] = evento["properties"]["event_timestamp"]
            context_info["event_name"] = "APP_STARTUP_TIME"
            context_info["event_p01"] = evento["properties"]["session_id"]
            context_info["event_p02"] = evento["info"]["app_code"]
            context_info["event_p03"] = context_info["app_name"]
            #todo verificare che la sottrazione tra i due timestamp sia un qualcosa di coerente
            #todo verificare correttezza ragionamento in agenda: norale che c'è gia un application_start_timestamp quando arriva screenloaded. potrei fare che lo leggo e poi lo imposto a "calcolato"
            context_info["event_p04"] = str(startup_time_ms)
            context_info["event_p05"] = ""
            context_info["event_p06"] = ""
            context_info["event_p07"] = ""
            context_info["event_p08"] = ""
            context_info["event_p09"] = ""
            context_info["event_p10"] = ""
            context_info["event_p11"] = ""
            context_info["event_p12"] = ""
            context_info["event_p13"] = ""
            context_info["event_p14"] = ""
            context_info["event_p15"] = ""
            messaggio = {"event_id": "ID-XXX", "schema_version": "V-XXXX", "ts": evento["properties"]["event_timestamp"], "entity_name": "TM",
                         "payload": context_info}
            TELEMETRIA.append(messaggio)

        def DL_TM_APP_API(context_info, evento, service_reply_timestamp):
            database = firestore.Client()
            a = database.collection(u'sessions').document(session_id).get()
            eventId = evento["properties"]["service_name"]
            service_request_ts = a._data[eventId]
            screen_name = a._data["last_screen_loaded_name"]
            service_reply_timestamp = datetime.datetime.strptime(service_reply_timestamp, "%Y-%m-%dT%H:%M:%SZ")
            service_request_ts = datetime.datetime.strptime(service_request_ts, "%Y-%m-%dT%H:%M:%SZ")
            api_response_time = (service_reply_timestamp-service_request_ts)/1000

            context_info["event_ts"] = evento["properties"]["event_timestamp"]
            context_info["event_name"] = "APP_API"
            context_info["event_p01"] = evento["properties"]["session_id"]
            context_info["event_p02"] = evento["info"]["app_code"]
            context_info["event_p03"] = context_info["app_name"]
            # todo verificare che la sottrazione tra i due timestamp sia un qualcosa di coerente
            # todo verificare correttezza ragionamento in agenda: norale che c'è gia un application_start_timestamp quando arriva screenloaded. potrei fare che lo leggo e poi lo imposto a "calcolato"
            context_info["event_p04"] = "act_type"
            context_info["event_p05"] = "api_call_id"
            context_info["event_p06"] = "api_id"
            context_info["event_p07"] = evento["properties"]["service_name"]
            context_info["event_p08"] = screen_name
            context_info["event_p09"] = str(api_response_time)
            context_info["event_p10"] = ""
            context_info["event_p11"] = ""
            context_info["event_p12"] = ""
            context_info["event_p13"] = ""
            context_info["event_p14"] = ""
            context_info["event_p15"] = ""
            messaggio = {"event_id": "ID-XXX", "schema_version": "V-XXXX", "ts": evento["properties"]["event_timestamp"], "entity_name": "TM",
                         "payload": context_info}
            TELEMETRIA.append(messaggio)

        def DL_TM_SERVICE_APP_ERROR(context_info, evento):
            database = firestore.Client()
            a = database.collection(u'sessions').document(session_id).get()
            screen_name = a._data["last_screen_loaded_name"]

            context_info["event_ts"] = evento["properties"]["event_timestamp"]
            context_info["event_name"] = "APP_ERROR"
            context_info["event_p01"] = evento["properties"]["session_id"]
            context_info["event_p02"] = evento["info"]["app_code"]
            context_info["event_p03"] = context_info["app_name"]
            context_info["event_p04"] = screen_name
            context_info["event_p05"] = "api_call_id"
            context_info["event_p06"] = "api_id"
            context_info["event_p07"] = evento["properties"]["service_name"]
            context_info["event_p08"] = evento["properties"]["error_type"]
            context_info["event_p09"] = evento["properties"]["error_code"]
            context_info["event_p10"] = evento["properties"]["error_info"]
            context_info["event_p11"] = ""
            context_info["event_p12"] = ""
            context_info["event_p13"] = ""
            context_info["event_p14"] = ""
            context_info["event_p15"] = ""
            messaggio = {"event_id": "ID-XXX", "schema_version": "V-XXXX", "ts": evento["properties"]["event_timestamp"], "entity_name": "TM",
                         "payload": context_info}
            TELEMETRIA.append(messaggio)

        def DL_TM_APPLICATION_APP_ERROR(context_info, evento):
            database = firestore.Client()
            a = database.collection(u'sessions').document(session_id).get()
            screen_name = a._data["last_screen_loaded_name"]

            context_info["event_ts"] = evento["properties"]["event_timestamp"]
            context_info["event_name"] = "APP_ERROR"
            context_info["event_p01"] = evento["properties"]["session_id"]
            context_info["event_p02"] = evento["info"]["app_code"]
            context_info["event_p03"] = context_info["app_name"]
            context_info["event_p04"] = screen_name
            context_info["event_p05"] = ""
            context_info["event_p06"] = ""
            context_info["event_p07"] = ""
            context_info["event_p08"] = evento["properties"]["error_type"]
            context_info["event_p09"] = evento["properties"]["error_code"]
            context_info["event_p10"] = evento["properties"]["error_info"]
            context_info["event_p11"] = ""
            context_info["event_p12"] = ""
            context_info["event_p13"] = ""
            context_info["event_p14"] = ""
            context_info["event_p15"] = ""
            messaggio = {"event_id": "ID-XXX", "schema_version": "V-XXXX",
                         "ts": evento["properties"]["event_timestamp"], "entity_name": "TM",
                         "payload": context_info}
            TELEMETRIA.append(messaggio)

        def DL_TM_APPLICATION_EXIT(context_info, evento):
            database = firestore.Client()
            a = database.collection(u'sessions').document(session_id).get()
            screen_name = a._data["last_screen_loaded_name"]

            context_info["event_ts"] = evento["properties"]["event_timestamp"]
            context_info["event_name"] = "APP_EXIT"
            context_info["event_p01"] = evento["properties"]["session_id"]
            context_info["event_p02"] = evento["info"]["app_code"]
            context_info["event_p03"] = context_info["app_name"]
            context_info["event_p04"] = screen_name
            context_info["event_p05"] = "closing_reason"
            context_info["event_p06"] = ""
            context_info["event_p07"] = ""
            context_info["event_p08"] = ""
            context_info["event_p09"] = ""
            context_info["event_p10"] = ""
            context_info["event_p11"] = ""
            context_info["event_p12"] = ""
            context_info["event_p13"] = ""
            context_info["event_p14"] = ""
            context_info["event_p15"] = ""
            messaggio = {"event_id": "ID-XXX", "schema_version": "V-XXXX",
                         "ts": evento["properties"]["event_timestamp"], "entity_name": "TM",
                         "payload": context_info}
            TELEMETRIA.append(messaggio)



        for x in range(len(eventsList)):
            SESSION = {'SESSION_BEGIN', 'SESSION_END'}
            nomeEvento = eventsList[x]["properties"]["event_name"]
            evento = eventsList[x]
            if nomeEvento in SESSION:
                sessionHandler(nomeEvento,evento)
            #todo else if evento == app start, che in tm è app entry (cose di fe-be) e fare come nell'handler. RISPETTARE L'ORDINE E NOTARE CXHE I CAMPI SONO OBBLIGATORI QUINDI SE NN LI SO:""
            #todo Ottieni tutti i documenti in una raccolta: https://firebase.google.com/docs/firestore/query-data/get-data
            if nomeEvento == "APPLICATION_START":
                session_id = eventsList[x]["properties"]["session_id"]
                context_info = getCollections(session_id)
                DL_TM_APP_ENTRY(context_info, evento)
            elif nomeEvento == "SCREEN_LOADED":
                #ogni volta che arriva questo evento devo salvare lo "screen name" dell'ev "screen loaded" in firestore per calcolare page_name (di app_api ad esempio)

                database = firestore.Client()
                doc_ref = database.collection(u'sessions').document(evento["properties"]["session_id"])
                lastScreenLoadedName = evento["properties"]["screen_name"]
                doc_ref.set({
                    u"last_screen_loaded_name": lastScreenLoadedName,
                }, merge=True)

                #PER APP_STARTUP_TIME: QUI VEDO IN FIRESTORE SE HO GIA UNO SCREEN_LOADED_TIME(se ce n'è già uno vuol dire che ho già' mandato l'evento) e prendere l' application_startup_timestamp
                session_id = eventsList[x]["properties"]["session_id"]
                a = database.collection(u'sessions').document(session_id).get()
                if( a._data["app_entry_timestamp"] != "null" ):
                    context_info = getCollections(session_id)
                    DL_TM_APP_STARTUP_TIME(context_info, evento, eventsList[x]["properties"]["event_timestamp"])

            elif nomeEvento == "SERVICE_REQUEST":
                # NEI METADATA FIRESTORE DI SESSIONE AGGIUNGERò ANCHE ---> ID SERVIZIO_REQUEST: TIMESTAMP
                database = firestore.Client()
                doc_ref = database.collection(u'sessions').document(evento["properties"]["session_id"])
                e = evento["properties"]["service_name"]
                a = evento["properties"]["event_timestamp"]
                eventId = evento["properties"]["service_name"]
                doc_ref.set({
                    eventId: evento["properties"]["event_timestamp"],
                }, merge=True)

            elif nomeEvento == "SERVICE_REPLY":
                #NEI METADATA FIRESTORE DI SESSIONE AGGIUNGERò ANCHE ---> ID SERVIZIO_REPLY: TIMESTAMP
                #QUI POSSO COMPLETARE app_api(SPEC FE --> SERVICE REQUEST, GESTITO GIU), IN PARTICOLARE "api_response_time".per quel service name prendo su firestore id_servizio_reply-id_servizio_request
                session_id = eventsList[x]["properties"]["session_id"]
                context_info = getCollections(session_id)
                DL_TM_APP_API(context_info, evento, eventsList[x]["properties"]["event_timestamp"])

            elif nomeEvento == "SERVICE_ERROR":
                #costruisco un apperror completo, trattando pagename come sopra
                session_id = eventsList[x]["properties"]["session_id"]
                context_info = getCollections(session_id)
                DL_TM_SERVICE_APP_ERROR(context_info, evento)

            elif nomeEvento == "APPLICATION_ERROR":
                # costruisco un apperror senza i 3 campi che riguardano api, trattando pagename come sopra
                session_id = eventsList[x]["properties"]["session_id"]
                context_info = getCollections(session_id)
                DL_TM_APPLICATION_APP_ERROR(context_info, evento)

            elif nomeEvento == "APPLICATION_EXIT":
                #TODO AGGIUSTARE LA QUESTIONE DELLE CONTEXT INFO, LE DEVO OTTENERE DI TUTTE LE SESISONI E METTERLE IN LOCALE E POI VEDERE QUAL'è IL CONTEXT INFO GIUSTO DELL'EVENTO OGNI VOLTA CHE ARRIVA
                #costruisco un APP_EXIT, trattando pagename come sopra
                session_id = eventsList[x]["properties"]["session_id"]
                context_info = getCollections(session_id)
                DL_TM_APPLICATION_EXIT(context_info, evento)


#        a = json.dumps(TELEMETRIA)
#        r = requests.post('http://127.0.0.1:8000/eve', data=a)

        currentDate = datetime.datetime.now().strftime("%Y-%m-%d")
        currentHour = datetime.datetime.now().strftime("%H")
        currentDateTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        with beam.io.gcp.gcsio.GcsIO().open(filename="gs://myaccount-analytics/"+currentDate+"/"+currentHour+"/MyAccount"+currentDateTime, mode="w") as f:
            if(len(TV_APPS)>0):
                f.write("{}\n".format(json.dumps(TV_APPS)).encode("utf-8"))
            if(len(TELEMETRIA)>0):
                f.write("{}\n".format(json.dumps(TELEMETRIA)).encode("utf-8"))
            if(len(DIGITAL_HUB)>0):
                f.write("{}\n".format(json.dumps(DIGITAL_HUB)).encode("utf-8"))


def run(input_topic, output_path, window_size=1.0, pipeline_args=None):

    # `save_main_session` is set to true because some DoFn's rely on
    # globally imported modules.

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read PubSub Messages"
            >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Window into" >> GroupWindowsIntoBatches(window_size)
            | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(output_path))
        )



if __name__ == "__main__":  # noqa
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in number of minutes.",
    )
    parser.add_argument(
        "--output_path",
        help="GCS Path of the output file including filename prefix.",
    )


    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_path,
        known_args.window_size,
        pipeline_args,
        )
