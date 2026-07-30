use std::collections::BTreeMap;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value;
use serde_json::json;

wit_bindgen::generate!({
    path: "wit",
    world: "reducer",
});

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct State {
    run_id: String,
    run: Option<Value>,
    #[serde(default)]
    steps: BTreeMap<String, Value>,
    #[serde(default)]
    hooks: BTreeMap<String, Value>,
    #[serde(default)]
    waits: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Intent {
    run_id: String,
    request: Value,
    event_id: String,
    synthetic_event_id: Option<String>,
    now: String,
    operation_id: String,
}

struct WorkflowReducer;

impl Guest for WorkflowReducer {
    fn reduce(state: Vec<u8>, intent: Vec<u8>, context: Context) -> Result<Reduction, String> {
        let mut state = if state.is_empty() {
            State::default()
        } else {
            serde_json::from_slice(&state).map_err(|error| error.to_string())?
        };
        let intent: Intent = serde_json::from_slice(&intent).map_err(|error| error.to_string())?;
        if state.run_id.is_empty() {
            state.run_id = intent.run_id.clone();
        } else if state.run_id != intent.run_id {
            return Err("run ID does not match reducer state".to_owned());
        }
        let (events, changes, response) = apply_event(&mut state, &intent)?;
        let mut commit = Map::from_iter([
            ("version".to_owned(), json!(1)),
            ("operationId".to_owned(), Value::String(intent.operation_id)),
            ("runId".to_owned(), Value::String(state.run_id.clone())),
            ("previousRecord".to_owned(), json!(context.next_record)),
            ("events".to_owned(), Value::Array(events)),
        ]);
        for (key, value) in changes {
            commit.insert(key, value);
        }
        Ok(Reduction {
            state: serde_json::to_vec(&state).map_err(|error| error.to_string())?,
            records: vec![
                serde_json::to_vec(&Value::Object(commit)).map_err(|error| error.to_string())?,
            ],
            response: serde_json::to_vec(&response).map_err(|error| error.to_string())?,
        })
    }
}

fn request_type(intent: &Intent) -> Result<&str, String> {
    intent
        .request
        .get("eventType")
        .and_then(Value::as_str)
        .ok_or_else(|| "intent request is missing eventType".to_owned())
}

fn request_data(intent: &Intent) -> Value {
    intent
        .request
        .get("eventData")
        .cloned()
        .unwrap_or(Value::Null)
}

fn correlation_id(intent: &Intent) -> Option<&str> {
    intent.request.get("correlationId").and_then(Value::as_str)
}

fn event(intent: &Intent, event_id: &str, event_type: Option<&str>, data: Value) -> Value {
    let mut value = intent.request.clone();
    let object = value.as_object_mut().expect("validated request object");
    object.insert(
        "eventType".to_owned(),
        Value::String(
            event_type
                .unwrap_or_else(|| request_type(intent).unwrap_or(""))
                .to_owned(),
        ),
    );
    object.insert("eventData".to_owned(), data);
    object.insert("runId".to_owned(), Value::String(intent.run_id.clone()));
    object.insert("eventId".to_owned(), Value::String(event_id.to_owned()));
    object.insert("createdAt".to_owned(), Value::String(intent.now.clone()));
    object.insert(
        "specVersion".to_owned(),
        intent
            .request
            .get("specVersion")
            .cloned()
            .unwrap_or(json!(1)),
    );
    value
}

fn entity_change(id: &str, value: Value) -> Value {
    json!({"id": id, "value": value})
}

fn set_field(value: &mut Value, key: &str, nested: Value) -> Result<(), String> {
    value
        .as_object_mut()
        .ok_or_else(|| "entity is not an object".to_owned())?
        .insert(key.to_owned(), nested);
    Ok(())
}

fn remove_field(value: &mut Value, key: &str) -> Result<(), String> {
    value
        .as_object_mut()
        .ok_or_else(|| "entity is not an object".to_owned())?
        .remove(key);
    Ok(())
}

fn apply_event(
    state: &mut State,
    intent: &Intent,
) -> Result<(Vec<Value>, Map<String, Value>, Value), String> {
    let event_type = request_type(intent)?;
    let data = request_data(intent);
    let mut changes = Map::new();

    if event_type == "run_created" {
        if state.run.is_some() {
            return Err("workflow run already exists".to_owned());
        }
        let mut run = json!({
            "runId": intent.run_id,
            "deploymentId": data.get("deploymentId").cloned().unwrap_or(Value::Null),
            "workflowName": data.get("workflowName").cloned().unwrap_or(Value::Null),
            "input": data.get("input").cloned().unwrap_or(Value::Null),
            "executionContext": data.get("executionContext").cloned().unwrap_or(Value::Null),
            "attributes": data.get("attributes").cloned().unwrap_or_else(|| json!({})),
            "status": "pending",
            "specVersion": intent.request.get("specVersion").cloned().unwrap_or(json!(1)),
            "createdAt": intent.now,
            "updatedAt": intent.now,
        });
        remove_null_fields(&mut run);
        state.run = Some(run.clone());
        changes.insert("run".to_owned(), run.clone());
        let committed_event = event(intent, &intent.event_id, None, data);
        return Ok((
            vec![committed_event.clone()],
            changes,
            json!({"event": committed_event, "run": run, "maxEvents": 25000}),
        ));
    }

    if event_type == "run_started" && state.run.is_none() {
        let synthetic_id = intent
            .synthetic_event_id
            .as_deref()
            .ok_or_else(|| "lazy run start requires syntheticEventId".to_owned())?;
        let mut run = json!({
            "runId": intent.run_id,
            "deploymentId": data.get("deploymentId").cloned().unwrap_or(Value::Null),
            "workflowName": data.get("workflowName").cloned().unwrap_or(Value::Null),
            "input": data.get("input").cloned().unwrap_or(Value::Null),
            "executionContext": data.get("executionContext").cloned().unwrap_or(Value::Null),
            "attributes": data.get("attributes").cloned().unwrap_or_else(|| json!({})),
            "status": "running",
            "specVersion": intent.request.get("specVersion").cloned().unwrap_or(json!(1)),
            "createdAt": intent.now,
            "updatedAt": intent.now,
            "startedAt": intent.now,
        });
        remove_null_fields(&mut run);
        state.run = Some(run.clone());
        changes.insert("run".to_owned(), run.clone());
        let created = event(
            intent,
            synthetic_id,
            Some("run_created"),
            json!({
                "deploymentId": run.get("deploymentId"),
                "workflowName": run.get("workflowName"),
                "input": run.get("input"),
                "executionContext": run.get("executionContext"),
                "attributes": run.get("attributes"),
            }),
        );
        let started = event(intent, &intent.event_id, None, data);
        return Ok((
            vec![created, started.clone()],
            changes,
            json!({"event": started, "run": run, "maxEvents": 25000}),
        ));
    }

    let mut run = state
        .run
        .clone()
        .ok_or_else(|| "workflow run not found".to_owned())?;
    let committed_event = event(intent, &intent.event_id, None, data.clone());
    let mut response = json!({"event": committed_event});
    let correlation = correlation_id(intent).map(str::to_owned);

    match event_type {
        "run_started" => {
            set_field(&mut run, "status", json!("running"))?;
            set_field(&mut run, "startedAt", json!(intent.now))?;
            set_field(&mut run, "updatedAt", json!(intent.now))?;
            state.run = Some(run.clone());
            changes.insert("run".to_owned(), run.clone());
            response["run"] = run;
        }
        "run_completed" | "run_failed" | "run_cancelled" => {
            let status = event_type.trim_start_matches("run_");
            set_field(&mut run, "status", json!(status))?;
            set_field(&mut run, "completedAt", json!(intent.now))?;
            set_field(&mut run, "updatedAt", json!(intent.now))?;
            if event_type == "run_completed" {
                set_field(
                    &mut run,
                    "output",
                    data.get("output").cloned().unwrap_or(Value::Null),
                )?;
            } else if event_type == "run_failed" {
                set_field(
                    &mut run,
                    "error",
                    data.get("error").cloned().unwrap_or(Value::Null),
                )?;
                set_field(
                    &mut run,
                    "errorCode",
                    data.get("errorCode").cloned().unwrap_or(Value::Null),
                )?;
            }
            remove_null_fields(&mut run);
            state.run = Some(run.clone());
            changes.insert("run".to_owned(), run.clone());
            response["run"] = run;
        }
        "step_created" => {
            let step_id = correlation.ok_or_else(|| "stepId is required".to_owned())?;
            if state.steps.contains_key(&step_id) {
                return Err("step already exists".to_owned());
            }
            let step = json!({
                "runId": intent.run_id,
                "stepId": step_id,
                "stepName": data.get("stepName").cloned().unwrap_or(Value::Null),
                "input": data.get("input").cloned().unwrap_or(Value::Null),
                "status": "pending",
                "attempt": 0,
                "specVersion": intent.request.get("specVersion").cloned().unwrap_or(json!(1)),
                "createdAt": intent.now,
                "updatedAt": intent.now,
            });
            state.steps.insert(step_id.clone(), step.clone());
            changes.insert(
                "steps".to_owned(),
                Value::Array(vec![entity_change(&step_id, step.clone())]),
            );
            response["step"] = step;
        }
        "step_started" => {
            let step_id = correlation.ok_or_else(|| "stepId is required".to_owned())?;
            let mut events = Vec::new();
            let mut step = if let Some(step) = state.steps.get(&step_id) {
                step.clone()
            } else {
                let synthetic_id = intent
                    .synthetic_event_id
                    .as_deref()
                    .ok_or_else(|| "lazy step start requires syntheticEventId".to_owned())?;
                response["stepCreated"] = Value::Bool(true);
                let created = event(
                    intent,
                    synthetic_id,
                    Some("step_created"),
                    json!({
                        "stepName": data.get("stepName"),
                        "input": data.get("input"),
                    }),
                );
                events.push(created);
                json!({
                    "runId": intent.run_id,
                    "stepId": step_id,
                    "stepName": data.get("stepName").cloned().unwrap_or(Value::Null),
                    "input": data.get("input").cloned().unwrap_or(Value::Null),
                    "status": "pending",
                    "attempt": 0,
                    "specVersion": intent.request.get("specVersion").cloned().unwrap_or(json!(1)),
                    "createdAt": intent.now,
                    "updatedAt": intent.now,
                })
            };
            let attempt = step
                .get("attempt")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .saturating_add(1);
            set_field(&mut step, "status", json!("running"))?;
            set_field(&mut step, "attempt", json!(attempt))?;
            if step.get("startedAt").is_none() {
                set_field(&mut step, "startedAt", json!(intent.now))?;
            }
            set_field(&mut step, "updatedAt", json!(intent.now))?;
            remove_field(&mut step, "retryAfter")?;
            state.steps.insert(step_id.clone(), step.clone());
            changes.insert(
                "steps".to_owned(),
                Value::Array(vec![entity_change(&step_id, step.clone())]),
            );
            response["step"] = step;
            events.push(committed_event);
            return Ok((events, changes, response));
        }
        "step_completed" | "step_failed" | "step_retrying" => {
            let step_id = correlation.ok_or_else(|| "stepId is required".to_owned())?;
            let mut step = state
                .steps
                .get(&step_id)
                .cloned()
                .ok_or_else(|| "step not found".to_owned())?;
            let status = match event_type {
                "step_completed" => "completed",
                "step_failed" => "failed",
                _ => "pending",
            };
            set_field(&mut step, "status", json!(status))?;
            set_field(&mut step, "updatedAt", json!(intent.now))?;
            if event_type == "step_completed" {
                set_field(
                    &mut step,
                    "output",
                    data.get("result").cloned().unwrap_or(Value::Null),
                )?;
                set_field(&mut step, "completedAt", json!(intent.now))?;
            } else {
                set_field(
                    &mut step,
                    "error",
                    data.get("error").cloned().unwrap_or(Value::Null),
                )?;
                if event_type == "step_failed" {
                    set_field(&mut step, "completedAt", json!(intent.now))?;
                } else if let Some(retry_after) = data.get("retryAfter") {
                    set_field(&mut step, "retryAfter", retry_after.clone())?;
                }
            }
            remove_null_fields(&mut step);
            state.steps.insert(step_id.clone(), step.clone());
            changes.insert(
                "steps".to_owned(),
                Value::Array(vec![entity_change(&step_id, step.clone())]),
            );
            response["step"] = step;
        }
        _ => return Err(format!("unsupported Workflow event type '{event_type}'")),
    }

    Ok((vec![committed_event], changes, response))
}

fn remove_null_fields(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        object.retain(|_, nested| !nested.is_null());
    }
}

export!(WorkflowReducer);
