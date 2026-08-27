defmodule Zaq.Agent.StreamEventsTest do
  use ExUnit.Case, async: true

  alias ReqLLM.ToolResult
  alias Zaq.Agent.StreamEvents
  alias Zaq.Contracts.Record
  alias Zaq.Engine.Messages.Incoming

  defmodule FakeStatus do
    def broadcast(incoming, stage, message, _node_router, opts) do
      send(self(), {:broadcast, stage, message, Keyword.get(opts, :update_intent)})
      put_in(incoming.metadata[:status_message_id], incoming.metadata[:request_id])
    end
  end

  defmodule StructEvent do
    defstruct [:kind, :at_ms, :iteration, :tool_call_id, :tool_name, :data]
  end

  defmodule JsonStruct do
    defstruct [:name]
  end

  test "coalesces small deltas and flushes the full content on terminal event" do
    incoming = incoming()

    events = [
      event(:llm_delta, 10, %{chunk_type: :content, delta: "hello ", model: "openai:gpt-4o-mini"}),
      event(:llm_delta, 20, %{chunk_type: :content, delta: "world", model: "openai:gpt-4o-mini"}),
      event(:request_completed, 30, %{
        result: "hello world",
        usage: %{input_tokens: 1, output_tokens: 2, total_tokens: 3}
      })
    ]

    assert {:ok, result} =
             StreamEvents.consume(events, incoming,
               status_module: FakeStatus,
               agent: %{id: 42, name: "Answering"}
             )

    assert result.answer == "hello world"
    assert result.usage == %{input_tokens: 1, output_tokens: 2, total_tokens: 3}

    assert [%{"id" => "llm-1", "type" => "content", "turn_id" => "0"} = trace_entry] =
             result.trace

    refute Map.has_key?(trace_entry, "content")

    assert result.measurements["total_tokens"] == 3
    assert result.measurements["input_tokens"] == 1
    assert result.measurements["output_tokens"] == 2
    refute Map.has_key?(result.measurements, "prompt_tokens")
    refute Map.has_key?(result.measurements, "completion_tokens")
    assert result.measurements["turn_count"] == 1
    assert result.measurements["llm_call_count"] == 1
    assert result.model == "openai:gpt-4o-mini"
    assert result.agent == %{id: 42, name: "Answering"}
    assert_receive {:broadcast, :answering, "hello world", :stream_delta}
    refute_receive {:broadcast, _, _, _}
  end

  test "orders sequenced runtime events before progressive broadcasts" do
    events = [
      event(:llm_delta, 300, %{chunk_type: :content, delta: "second."})
      |> Map.put(:seq, 3),
      event(:request_started, 100, %{}) |> Map.put(:seq, 1),
      event(:llm_delta, 200, %{
        chunk_type: :content,
        delta: "First ordered fragment ",
        model: "openai:gpt-oss-120b"
      })
      |> Map.put(:seq, 2),
      event(:request_completed, 400, %{result: "First ordered fragment second."})
      |> Map.put(:seq, 4)
    ]

    assert {:ok, result} =
             StreamEvents.consume(events, incoming(),
               status_module: FakeStatus,
               started_at: 0
             )

    assert result.answer == "First ordered fragment second."
    assert_receive {:broadcast, :answering, "First ordered fragment ", :stream_delta}
    assert_receive {:broadcast, :answering, "First ordered fragment second.", :stream_delta}
    refute_receive {:broadcast, _, _, _}
  end

  test "uses only the terminal answer when the request start never arrives" do
    events = [
      event(:llm_delta, 300, %{chunk_type: :content, delta: "second."})
      |> Map.put(:seq, 4),
      event(:request_completed, 400, %{result: "First ordered fragment second."})
      |> Map.put(:seq, 5)
    ]

    assert {:ok, result} =
             StreamEvents.consume(events, incoming(),
               status_module: FakeStatus,
               started_at: 0
             )

    assert result.answer == "First ordered fragment second."
    refute_receive {:broadcast, _, _, _}
  end

  test "uses canonical failure usage when failure overtakes the request start" do
    events = [
      event(:llm_delta, 300, %{chunk_type: :content, delta: "incomplete suffix"})
      |> Map.put(:seq, 4),
      event(:request_failed, 500, %{
        error: :worker_crashed,
        usage: %{input_tokens: 7, output_tokens: 2, total_tokens: 9}
      })
      |> Map.put(:seq, 5)
    ]

    assert {:error, :worker_crashed, result} =
             StreamEvents.consume(events, incoming(),
               status_module: FakeStatus,
               started_at: 0
             )

    assert result.answer == ""
    assert result.usage == %{input_tokens: 7, output_tokens: 2, total_tokens: 9}
    refute_receive {:broadcast, _, _, _}
  end

  test "preserves completed usage when cancellation overtakes the request start" do
    events = [
      event(:llm_delta, 300, %{chunk_type: :content, delta: "incomplete suffix"})
      |> Map.put(:seq, 4),
      event(:llm_completed, 400, %{usage: %{input_tokens: 5, output_tokens: 1}})
      |> Map.put(:seq, 5),
      event(:request_cancelled, 500, %{reason: "user stopped"}) |> Map.put(:seq, 0)
    ]

    assert {:ok, result} =
             StreamEvents.consume(events, incoming(),
               status_module: FakeStatus,
               started_at: 0
             )

    assert result.answer == ""
    assert result.termination_reason == :cancelled
    assert result.usage == %{input_tokens: 5, output_tokens: 1, total_tokens: 6}
    refute_receive {:broadcast, _, _, _}
  end

  test "discards noncontiguous progressive events when terminal overtakes a gap" do
    events = [
      event(:request_started, 100, %{}) |> Map.put(:seq, 1),
      event(:llm_delta, 200, %{
        chunk_type: :content,
        delta: "Ordered progressive prefix. "
      })
      |> Map.put(:seq, 2),
      event(:llm_delta, 300, %{chunk_type: :content, delta: "out-of-order fragment"})
      |> Map.put(:seq, 4),
      event(:request_completed, 400, %{
        result: "Ordered progressive prefix. Recovered terminal ending."
      })
      |> Map.put(:seq, 5)
    ]

    assert {:ok, result} =
             StreamEvents.consume(events, incoming(),
               status_module: FakeStatus,
               started_at: 0
             )

    assert result.answer == "Ordered progressive prefix. Recovered terminal ending."
    assert_receive {:broadcast, :answering, "Ordered progressive prefix. ", :stream_delta}

    refute_receive {:broadcast, :answering, "Ordered progressive prefix. out-of-order fragment",
                    :stream_delta}
  end

  test "uses llm_completed usage when terminal usage is empty" do
    events = [
      event(:llm_completed, 20, %{
        text: "hello world",
        usage: %{input_tokens: 3, output_tokens: 1}
      }),
      event(:request_completed, 30, %{result: "hello world", usage: %{}})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert result.usage.input_tokens == 3
    assert result.usage.output_tokens == 1
    assert result.measurements["input_tokens"] == 3
    assert result.measurements["output_tokens"] == 1
    assert result.measurements["total_tokens"] == 4
  end

  test "uses empty usage when llm_completed carries no usage map" do
    events = [
      event(:llm_completed, 10, %{usage: nil}),
      event(:request_completed, 20, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert result.answer == "done"
    assert result.usage == %{}
  end

  test "merges numeric token usage after ignoring invalid token values" do
    events = [
      event(:llm_completed, 10, %{usage: %{"input_tokens" => "n/a"}}),
      event(:llm_completed, 20, %{usage: %{"input_tokens" => 4}}),
      event(:request_completed, 30, %{result: "done", usage: %{}})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert result.usage.input_tokens == 4
    assert result.measurements["input_tokens"] == 4
    assert is_nil(result.measurements["total_tokens"])
  end

  test "normalizes string token keys and numeric usage values" do
    events = [
      event(:llm_completed, 10, %{
        usage: %{
          "input_tokens" => "2",
          "output_tokens" => "3",
          "inputTokenCount" => "5",
          "outputTokenCount" => "7"
        }
      }),
      event(:request_completed, 20, %{result: "done", usage: %{}})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert result.usage.input_tokens == 2
    assert result.usage.output_tokens == 3
    assert result.usage.inputTokenCount == 5
    assert result.usage.outputTokenCount == 7
    assert result.measurements["input_tokens"] == 2
    assert result.measurements["output_tokens"] == 3
    assert result.measurements["total_tokens"] == 5
  end

  test "normalizes provider token usage shapes from llm_completed" do
    events = [
      event(:llm_completed, 20, %{
        usage: %{
          "tokens" => %{
            "promptTokenCount" => "8.0",
            "candidatesTokenCount" => 3.9
          },
          "totalTokenCount" => "11.0"
        }
      }),
      event(:request_completed, 30, %{result: "done", usage: %{}})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert result.measurements["input_tokens"] == 8
    assert result.measurements["output_tokens"] == 3
    assert result.measurements["total_tokens"] == 11
  end

  test "flushes when chunk type changes and stores thinking as reasoning trace" do
    incoming = incoming()

    events = [
      event(:llm_delta, 150, %{chunk_type: :thinking, delta: "I should search first."}),
      event(:llm_delta, 300, %{chunk_type: :content, delta: "The answer is 42."}),
      event(:request_completed, 350, %{result: "The answer is 42."})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming, status_module: FakeStatus)

    assert [
             %{"type" => "reasoning", "content" => "I should search first."},
             %{"type" => "content"} = content_entry
           ] = result.trace

    refute Map.has_key?(content_entry, "content")

    assert_receive {:broadcast, :thinking, "I should search first.", :reasoning}
    assert_receive {:broadcast, :answering, "The answer is 42.", :stream_delta}
  end

  test "json_safe converts non-json values before trace persistence" do
    value = %{
      ok_tuple: {:ok, %{ref: make_ref()}},
      at: ~U[2026-06-11 10:00:00Z],
      nested: %{self: self()}
    }

    safe = StreamEvents.json_safe(value)

    assert Jason.encode!(safe)
    assert safe["ok_tuple"] |> is_list()
    assert safe["at"] == "2026-06-11T10:00:00Z"
    assert is_binary(safe["nested"]["self"])
  end

  test "json_safe converts structs without recursing forever" do
    safe = StreamEvents.json_safe(%JsonStruct{name: :ok})

    assert safe["name"] == "ok"
    assert safe["__struct__"] == Atom.to_string(JsonStruct)
    assert Jason.encode!(safe)
  end

  test "captures tool calls in trace and result" do
    incoming = incoming()

    events = [
      event(:tool_started, 10, %{tool_name: "search", arguments: %{query: "zaq"}},
        tool_call_id: "tool-1"
      ),
      event(:tool_completed, 20, %{tool_name: "search", result: %{hits: 2}, duration_ms: 10},
        tool_call_id: "tool-1"
      ),
      event(:request_completed, 30, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming, status_module: FakeStatus)

    assert [%{"id" => "tool-1", "response" => %{"hits" => 2}}] = result.tool_calls

    assert [%{"id" => "tool-1", "type" => "tool_call", "response" => %{"hits" => 2}}] =
             result.trace

    assert result.measurements["tool_call_count"] == 1
    assert_receive {:broadcast, :tool_call, "Using search", :tool_call}
    assert_receive {:broadcast, :tool_call, "Finished search", :tool_call}
  end

  test "externalizes materialized media from canonical tool results without adding bytes to trace" do
    bytes = <<0, 1, 2, 3>>

    record = %Record{
      id: "media-1",
      kind: :file,
      content: bytes,
      name: "image.png",
      mime_type: "image/png",
      size: 4,
      attributes: %{"source_type" => "communication_media"}
    }

    events = [
      event(:tool_started, 10, %{tool_name: "download_document", arguments: %{}},
        tool_call_id: "tool-media"
      ),
      event(
        :tool_completed,
        20,
        %{
          tool_name: "download_document",
          result: {:ok, %ToolResult{output: %{record: record}, content: []}, []}
        },
        tool_call_id: "tool-media"
      ),
      event(:request_completed, 30, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [artifact] = result.trace_artifacts
    assert artifact.content == bytes
    assert artifact.record["id"] == "media-1"
    refute Map.has_key?(artifact.record, "content")
    assert Jason.encode!(result.trace)
    refute inspect(result.trace) =~ inspect(bytes)
  end

  test "uses struct event timestamps for tool started entries" do
    events = [
      %StructEvent{
        kind: :tool_started,
        at_ms: 1_765_411_200_000,
        iteration: 0,
        tool_call_id: "struct-tool",
        data: %{tool_name: "lookup"}
      },
      event(:request_completed, 30, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [%{"id" => "struct-tool", "started_at" => started_at}] = result.tool_calls
    assert is_binary(started_at)
  end

  test "keeps tool started timestamps nil when the map has no at_ms field" do
    events = [
      %{
        kind: :tool_started,
        iteration: 0,
        tool_call_id: "map-tool",
        data: %{tool_name: "lookup"}
      },
      event(:request_completed, 20, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [%{"id" => "map-tool", "started_at" => nil}] = result.tool_calls
  end

  describe "terminal events" do
    test "keeps a synthetic failure whose sequence is stale" do
      events = [
        event(:request_started, 100, %{}) |> Map.put(:seq, 1),
        event(:llm_delta, 200, %{
          chunk_type: :content,
          delta: "Ordered partial answer before failure."
        })
        |> Map.put(:seq, 2),
        event(:request_failed, 300, %{error: :worker_crashed}) |> Map.put(:seq, 0)
      ]

      assert {:error, :worker_crashed, result} =
               StreamEvents.consume(events, incoming(),
                 status_module: FakeStatus,
                 started_at: 0
               )

      assert result.answer == "Ordered partial answer before failure."

      assert_receive {:broadcast, :answering, "Ordered partial answer before failure.",
                      :stream_delta}
    end

    test "keeps a synthetic cancellation whose sequence is stale" do
      events = [
        event(:request_started, 100, %{}) |> Map.put(:seq, 1),
        event(:llm_delta, 200, %{chunk_type: :content, delta: "Partial before cancellation."})
        |> Map.put(:seq, 2),
        event(:request_cancelled, 300, %{reason: "user stopped"}) |> Map.put(:seq, 0)
      ]

      assert {:ok, result} =
               StreamEvents.consume(events, incoming(),
                 status_module: FakeStatus,
                 started_at: 0
               )

      assert result.answer == "Partial before cancellation."
      assert result.termination_reason == :cancelled
      assert_receive {:broadcast, :answering, "Partial before cancellation.", :stream_delta}
    end

    test "returns cancelled result and registers cancellation" do
      incoming = incoming()

      events = [
        event(:llm_delta, 10, %{chunk_type: :content, delta: "partial"}),
        event(:request_cancelled, 20, %{reason: "user stopped"})
      ]

      assert {:ok, result} =
               StreamEvents.consume(events, incoming, status_module: FakeStatus)

      assert result.answer == "partial"
      assert result.termination_reason == :cancelled
      assert [%{"type" => "content"}] = result.trace
      assert_receive {:broadcast, :answering, "partial", :stream_delta}
    end
  end

  test "uses message_id as request id when metadata is not a map" do
    incoming = %Incoming{
      content: "question",
      channel_id: "chan-1",
      provider: :web,
      message_id: "msg-1",
      metadata: nil
    }

    events = [event(:request_completed, 30, %{result: "done"})]

    assert {:ok, result} = StreamEvents.consume(events, incoming, status_module: FakeStatus)

    assert result.answer == "done"
    assert result.incoming.message_id == "msg-1"
  end

  test "omits trace timestamps and duration when event timing is missing" do
    events = [
      %{
        kind: :llm_delta,
        data: %{chunk_type: :thinking, delta: "thinking"},
        iteration: 0,
        llm_call_id: nil
      },
      %{kind: :request_completed, data: %{result: "done"}}
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [%{"type" => "reasoning", "content" => "thinking"} = trace] = result.trace
    refute Map.has_key?(trace, "started_at")
    refute Map.has_key?(trace, "started_at_ms")
    refute Map.has_key?(trace, "ended_at")
    refute Map.has_key?(trace, "ended_at_ms")
    refute Map.has_key?(trace, "duration_ms")
  end

  test "accepts string-keyed tool events and struct-like event timestamps" do
    events = [
      %{
        "id" => "tool-string",
        "kind" => :tool_started,
        "at_ms" => 1_765_411_200_000,
        "iteration" => 1,
        "tool_call_id" => "tool-string",
        "data" => %{"tool_name" => "lookup", "arguments" => %{"q" => "zaq"}}
      },
      %{
        "id" => "tool-string",
        "kind" => :tool_completed,
        "at_ms" => 1_765_411_200_050,
        "iteration" => 1,
        "tool_call_id" => "tool-string",
        "data" => %{
          "tool_name" => "lookup",
          "result" => %{"ok" => true},
          "duration_ms" => 50
        }
      },
      event(:request_completed, 30, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [%{"id" => "tool-string", "started_at" => started, "ended_at" => ended}] =
             result.trace

    assert is_binary(started)
    assert is_binary(ended)
    assert [%{"duration_ms" => 50}] = result.tool_calls
  end

  test "drops invalid unix millisecond timestamps" do
    events = [
      event(:tool_started, 99_999_999_999_999_999_999, %{tool_name: "lookup"}),
      event(:request_completed, 1, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [%{"id" => _} = tool] = result.tool_calls
    assert is_nil(tool["started_at"])
    assert tool["started_at_ms"] == 99_999_999_999_999_999_999
  end

  test "normalizes string chunk types" do
    events = [
      event(:llm_delta, 10, %{chunk_type: "thinking", delta: "reason"}),
      event(:llm_delta, 120, %{chunk_type: "content", delta: "answer"}),
      event(:request_completed, 130, %{result: "answer"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert [%{"type" => "reasoning"}, %{"type" => "content"}] = result.trace
    assert_receive {:broadcast, :thinking, "reason", :reasoning}
    assert_receive {:broadcast, :answering, "answer", :stream_delta}
  end

  test "ignores malformed non-map events" do
    events = [
      :not_an_event,
      event(:request_completed, 10, %{result: "done"})
    ]

    assert {:ok, result} = StreamEvents.consume(events, incoming(), status_module: FakeStatus)

    assert result.answer == "done"
  end

  test "json_safe converts date/time, floats, opaque values, and non-string map keys" do
    pid = self()
    ref = make_ref()

    value = %{
      {:tuple, :key} => pid,
      123 => :number_key,
      date: ~D[2026-06-11],
      naive: ~N[2026-06-11 10:00:00],
      time: ~T[10:00:00],
      float: 1.5,
      opaque: %{pid: pid, ref: ref}
    }

    safe = StreamEvents.json_safe(value)

    assert safe["date"] == "2026-06-11"
    assert safe["naive"] == "2026-06-11T10:00:00"
    assert safe["time"] == "10:00:00"
    assert safe["float"] == 1.5
    assert is_binary(safe["{:tuple, :key}"])
    assert safe["123"] == "number_key"
    assert is_binary(safe["opaque"]["pid"])
    assert is_binary(safe["opaque"]["ref"])
    assert Jason.encode!(safe)
  end

  test "json_safe falls back to inspect for bitstrings" do
    safe = StreamEvents.json_safe(<<1::1>>)

    assert is_binary(safe)
    assert String.starts_with?(safe, "<<1::size(1)>>")
    assert Jason.encode!(safe)
  end

  test "json_safe falls back to inspect when sanitized value still cannot encode" do
    safe = StreamEvents.json_safe(%{bad: <<255>>})

    assert Jason.encode!(safe)
  end

  defp incoming do
    %Incoming{
      content: "question",
      channel_id: "chan-1",
      provider: :web,
      metadata: %{request_id: "req-1", session_id: "sess-1"}
    }
  end

  defp event(kind, at_ms, data, attrs \\ []) do
    %{
      id: "evt-#{at_ms}",
      seq: nil,
      at_ms: at_ms,
      run_id: "run-1",
      request_id: "req-1",
      iteration: 0,
      kind: kind,
      llm_call_id: "llm-1",
      tool_call_id: Keyword.get(attrs, :tool_call_id),
      tool_name: Map.get(data, :tool_name),
      data: data
    }
  end
end
