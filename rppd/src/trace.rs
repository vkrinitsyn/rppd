//! [otel] rppd's side of a pipeline trace (yt-pgxt resilience.md phase 4): a pp's
//! call is a span in the trace of the event that fired it - the committing cluster
//! transaction's, when the host's `on_commit_python` handed it on with its
//! `traceparent` - and the function sees its own span as `TRACE`, so its spans or
//! an outgoing call's header continue the same trace.

use opentelemetry::KeyValue;
use opentelemetry::trace::{Span as _, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer};
use opentelemetry_sdk::trace::{SdkTracer, Span};

use crate::rd_fn::RpFnLog;

/// The span of a call of pp `pp` on node `node`, a child of its event's trace
pub(crate) fn pp_span(t: &SdkTracer, name: String, f: &RpFnLog, pp: i32, node: i32) -> Span {
    let b = t.span_builder(name).with_attributes(vec![
        KeyValue::new("rppd.pp", pp as i64),
        KeyValue::new("rppd.node", node as i64),
        KeyValue::new("rppd.attempt", (f.attempt + 1) as i64),
        KeyValue::new("rppd.trig_type", f.trig_type as i64),
    ]);
    match f.traceparent.as_deref().and_then(parent) {
        Some(cx) => b.start_with_context(t, &cx),
        None => b.start(t),
    }
}

/// W3C `traceparent` of a span
pub(crate) fn traceparent_of(s: &Span) -> Option<String> {
    let sc = s.span_context();
    sc.is_valid().then(|| format!("00-{}-{}-{:02x}", sc.trace_id(), sc.span_id(), sc.trace_flags().to_u8()))
}

/// A W3C `traceparent` as a remote parent
pub(crate) fn parent(tp: &str) -> Option<opentelemetry::Context> {
    let p: Vec<&str> = tp.trim().split('-').collect();
    if p.len() < 4 || p[0] != "00" {
        return None;
    }
    let sc = SpanContext::new(TraceId::from_hex(p[1]).ok()?, SpanId::from_hex(p[2]).ok()?,
        TraceFlags::new(u8::from_str_radix(p[3], 16).ok()?), true, TraceState::default());
    sc.is_valid().then(|| opentelemetry::Context::new().with_remote_span_context(sc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider;

    /// a pp's span continues its event's trace, and TRACE names the span itself
    #[test]
    fn a_call_continues_the_event_trace() {
        let p = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let t = p.tracer("rppd");
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string();
        let f = RpFnLog { traceparent: Some(tp), ..RpFnLog::default() };
        let s = pp_span(&t, "public.t".into(), &f, 7, 2);
        let own = traceparent_of(&s).unwrap();
        assert!(own.starts_with("00-0af7651916cd43dd8448eb211c80319c-"), "same trace: {}", own);
        assert!(!own.contains("b7ad6b7169203331"), "its own span id: {}", own);
        // no trace on the event: a root of its own
        let root = pp_span(&t, "public.t".into(), &RpFnLog::default(), 7, 2);
        assert!(!traceparent_of(&root).unwrap().contains("0af7651916cd43dd8448eb211c80319c"));
        assert!(parent("nonsense").is_none());
    }
}
