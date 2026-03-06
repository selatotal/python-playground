import os
import sys
import logging
import requests
import boto3
from datetime import date
from jira import JIRA

from datadog_api_client import ApiClient, Configuration
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)
logger = logging.getLogger(__name__)

def parse_monday(date_str):
    tz = ZoneInfo("America/Sao_Paulo")
    monday = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
    sunday = monday + timedelta(days=6)
    sunday = sunday.replace(hour=23, minute=59, second=59)
    return monday, sunday

def month_range_from_date(d):
    start = d.replace(day=1)
    end = d
    return start, end

def get_datadog_client():
    configuration = Configuration()
    return ApiClient(configuration)

def get_datadog_utilization(start_ts, end_ts, env="prod"):
    from datadog_api_client.v1.api.metrics_api import MetricsApi

    with get_datadog_client() as api_client:
        api = MetricsApi(api_client)

        def query_scalar(q):
            resp = api.query_metrics(
                _from=int(start_ts),
                to=int(end_ts),
                query=q
            )

            if not resp.series or not resp.series[0].pointlist:
                logger.debug("Sem pontos para: %s", q)
                return 0.0

            p = resp.series[0].pointlist[-1]

            if hasattr(p, "value"):
                v = p.value
            elif isinstance(p, (list, tuple)):
                if len(p) >= 2:
                    v = p[1]
                elif len(p) == 1:
                    v = p[0]
                else:
                    return 0.0
            else:
                return 0.0

            if isinstance(v, (list, tuple)):
                nums = [x for x in v if isinstance(x, (int, float))]
                if not nums:
                    return 0.0
                return float(nums[-1])

            # Se for número simples
            if isinstance(v, (int, float)):
                return float(v)

            return 0.0

        memory_usage = query_scalar(f"sum:kubernetes.memory.working_set{{env:{env}}}")
        memory_requests = query_scalar(f"sum:kubernetes.memory.requests{{env:{env}}}")
        cpu_usage = query_scalar(f"sum:kubernetes.cpu.usage.total{{env:{env}}}")
        cpu_requests = query_scalar(f"sum:kubernetes.cpu.requests{{env:{env}}}")

        memory = (memory_usage / memory_requests) * 100 if memory_requests else 0
        cpu = (cpu_usage / (cpu_requests * 10000000)) if cpu_requests else 0
        result = memory * 0.8 + cpu * 0.2

        logger.debug("A=%s, B=%s  C=%s, D=%s", memory_usage, memory_requests, cpu_usage, cpu_requests)
        logger.debug("Memory=%.2f%%, CPU=%.2f%%, Total=%.2f%%", memory, cpu, result)

        return round(result, 2)

def get_aws_month_cost(profile):
    session = boto3.Session(profile_name=profile)
    ce = session.client("ce")

    today = date.today()

    start = today

    if today.month == 12:
        end = today.replace(year=today.year + 1, month=1, day=1)
    else:
        end = today.replace(month=today.month + 1, day=1)

    resp = ce.get_cost_forecast(
        TimePeriod={
            "Start": start.strftime("%Y-%m-%d"),
            "End": end.strftime("%Y-%m-%d"),
        },
        Metric="UNBLENDED_COST",
        Granularity="MONTHLY",
    )

    amount = resp["Total"]["Amount"]
    return round(float(amount), 2)

def get_security_score(profile, region="us-east-1"):
    import boto3

    session = boto3.Session(profile_name=profile, region_name=region)
    sh = session.client("securityhub")

    paginator = sh.get_paginator("get_findings")

    controls = {}

    pages = 0
    findings_seen = 0

    for page in paginator.paginate(
            Filters={
                "RecordState": [{"Value": "ACTIVE", "Comparison": "EQUALS"}]
            },
            PaginationConfig={"PageSize": 100}
    ):
        pages += 1
        for finding in page.get("Findings", []):
            findings_seen += 1

            compliance = finding.get("Compliance")
            if not compliance:
                continue

            control_id = compliance.get("SecurityControlId")
            if not control_id:
                continue

            status = compliance.get("Status")

            if status not in ("PASSED", "FAILED"):
                continue

            if control_id not in controls:
                controls[control_id] = status
            else:
                if controls[control_id] == "PASSED" and status == "FAILED":
                    controls[control_id] = "FAILED"

    passed = sum(1 for s in controls.values() if s == "PASSED")
    failed = sum(1 for s in controls.values() if s == "FAILED")

    logger.debug("%s: pages=%s findings=%s controls=%s passed=%s failed=%s", 
                     profile, pages, findings_seen, len(controls), passed, failed)

    total = passed + failed
    if total == 0:
        logger.warning("Nenhum control avaliável em %s", profile)
        return 0.0

    score = (passed / total) * 100
    return round(score, 2)

def get_sla():
    url = "https://status.nexti.com/api/getMonitorList/08yZpTjgL"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    return float(data["statistics"]["uptime"]["l7"]["ratio"])

def get_jira():
    return JIRA(
        server=os.environ["JIRA_URL"],
        basic_auth=(os.environ["JIRA_EMAIL"], os.environ["JIRA_TOKEN"]),
    )

def get_jira_issues(start, end):
    jira = get_jira()

    start_str = start.strftime("%Y-%m-%d %H:%M")
    end_str = end.strftime("%Y-%m-%d %H:%M")

    jql = f"""
    assignee = "{os.environ['JIRA_EMAIL']}"
    AND (status = Done OR status = Arquivar OR status = Archived)
    AND resolved >= "{start_str}"
    AND resolved <= "{end_str}"
    """

    issues = jira.search_issues(jql, maxResults=1000)
    return issues

def compute_on_time_and_points(issues):
    total = 0
    on_time = 0
    points = 0
    issue_details = []

    score_map = {
        "PP": 1,
        "P": 2,
        "M": 3.5,
        "G": 6,
        "GG": 10,
    }

    for issue in issues:
        total += 1
        fields = issue.fields

        due = fields.duedate
        resolved = fields.resolutiondate

        resolved_date = None
        due_date = None
        status_label = "Sem prazo"
        issue_points = 0

        if resolved:
            resolved_date = datetime.strptime(resolved[:10], "%Y-%m-%d").date()

        if due:
            due_date = datetime.strptime(due, "%Y-%m-%d").date()

        if due_date and resolved_date:
            if resolved_date <= due_date:
                on_time += 1
                status_label = "No prazo"
            else:
                status_label = "Atrasado"

        effort = getattr(fields, "customfield_10637", None)
        if effort is not None and effort.value in score_map:
            issue_points = score_map[effort.value]
            points += issue_points

        issue_details.append({
            "key": issue.key,
            "summary": fields.summary,
            "resolved": resolved_date,
            "due": due_date,
            "status": status_label,
            "points": issue_points
        })

    on_time_pct = round((on_time / total) * 100, 2) if total else 0

    return on_time_pct, points, issue_details

def get_google_calendar_service():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    import os.path

    SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def get_meeting_hours_not_organized_by(start, end):
    service = get_google_calendar_service()

    whitelist_raw = os.environ.get("ORGANIZER_WHITELIST", "")
    whitelist = {e.strip().lower() for e in whitelist_raw.split(",") if e.strip()}

    events_result = service.events().list(
        calendarId="primary",
        timeMin=start.isoformat(),
        timeMax=end.isoformat(),
        singleEvents=True,
        orderBy="startTime",
    ).execute()

    events = events_result.get("items", [])

    total_seconds = 0
    meetings_count = 0

    for event in events:
        if "dateTime" not in event.get("start", {}):
            continue

        organizer_email = (
            event.get("organizer", {})
            .get("email", "")
            .lower()
        )

        if organizer_email in whitelist:
            continue

        start_dt = datetime.fromisoformat(event["start"]["dateTime"])
        end_dt = datetime.fromisoformat(event["end"]["dateTime"])

        duration = (end_dt - start_dt).total_seconds()

        total_seconds += duration
        meetings_count += 1

    hours = round(total_seconds / 3600, 2)

    return hours, meetings_count

def main():
    if len(sys.argv) != 2:
        logger.error("Uso: python kpis.py YYYY-MM-DD")
        sys.exit(1)

    monday, sunday = parse_monday(sys.argv[1])

    logger.info("Período: %s até %s", monday, sunday)

    start_ts = int(datetime.combine(monday, datetime.min.time()).timestamp())
    end_ts = int(datetime.combine(sunday, datetime.max.time()).timestamp())

    logger.debug("Timestamps: %s até %s", start_ts, end_ts)

    utilization = get_datadog_utilization(start_ts, end_ts)
    aws_identity_cost = get_aws_month_cost(os.environ["AWS_PAYER_PROFILE"])
    aws_prod_cost = get_aws_month_cost(os.environ["AWS_PROD_PROFILE"])
    sla = get_sla()

    issues = get_jira_issues(monday, sunday)
    on_time_pct, dev_points, issue_details = compute_on_time_and_points(issues)

    sec_prod = get_security_score("nexti-prod-terraform")
    sec_qa = get_security_score("nexti-qa-terraform")

    meeting_hours, meeting_count = get_meeting_hours_not_organized_by(monday, sunday)

    logger.info("================= KPI SEMANAL =================")
    logger.info("Período: %s até %s", monday, sunday)

    logger.info("[CLOUD]")
    logger.info("- Taxa de utilização da plataforma: %.2f %%", utilization)
    logger.info("- Custo mensal AWS (Conta Payer):     R$ {:,.2f}".format(aws_identity_cost))
    logger.info("- Custo mensal AWS (Prod):            R$ {:,.2f}".format(aws_prod_cost))
    logger.info("- Custo mensal Datadog:               MANUAL")
    logger.info("- SLA Disponibilidade:                %.3f %%", sla)

    logger.info("[SEGURANÇA]")
    logger.info("- Security Score PROD: %.2f %%", sec_prod)
    logger.info("- Security Score QA:   %.2f %%", sec_qa)

    logger.info("[ENTREGA]")
    logger.info("- On-Time Delivery:   %.2f %%", on_time_pct)
    logger.info("- Pontuação semanal:  %s pontos", dev_points)

    logger.info("Issues consideradas:")

    if not issues:
        logger.info("Nenhuma issue encontrada no período.")
    else:
        for i in issue_details:
            logger.info(
                "- %s | %s | Resolvida: %s | Due: %s | %s | %s pts",
                i["key"],
                i["summary"],
                i["resolved"],
                i["due"],
                i["status"],
                i["points"]
            )

    logger.info("[AGENDA]")
    logger.info("- Reuniões não organizadas por mim: %s", meeting_count)
    logger.info("- Horas em reuniões de apoio técnico: %.2f h", meeting_hours)
    logger.info("- Pontuação de reuniões: %.2f", (meeting_hours * 0.5))
    logger.info("==============================================")
    logger.info("- Pontuação de Entrega: %.2f", ((meeting_hours * 0.5) + dev_points))
    logger.info("==============================================")


if __name__ == "__main__":
    main()
