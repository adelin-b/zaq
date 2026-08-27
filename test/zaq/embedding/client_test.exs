defmodule Zaq.Embedding.ClientTest do
  use Zaq.DataCase, async: false

  import ExUnit.CaptureLog

  alias Zaq.Embedding.Client
  alias Zaq.SystemConfigFixtures

  defmodule RealHTTPStub do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, opts) do
      conn =
        Enum.reduce(Keyword.get(opts, :headers, []), conn, fn {k, v}, acc ->
          put_resp_header(acc, k, v)
        end)

      status = Keyword.get(opts, :status, 200)
      body = Keyword.get(opts, :body, %{})

      conn
      |> put_resp_content_type("application/json")
      |> send_resp(status, Jason.encode!(body))
    end
  end

  setup do
    SystemConfigFixtures.seed_embedding_config(%{
      endpoint: "http://localhost",
      model: "test-model",
      dimension: 1536
    })

    :ok
  end

  describe "config readers" do
    test "reads endpoint from config" do
      assert Client.endpoint() == "http://localhost"
    end

    test "reads model from config" do
      assert Client.model() == "test-model"
    end

    test "reads dimension from config" do
      assert Client.dimension() == 1536
    end

    test "reads api_key from config" do
      SystemConfigFixtures.seed_embedding_config(%{
        endpoint: "http://localhost",
        api_key: "sk-test-key",
        model: "test-model",
        dimension: 1536
      })

      assert Client.api_key() == "sk-test-key"
    end
  end

  describe "embed/2" do
    test "returns embedding on successful response" do
      embedding = List.duplicate(0.1, 10)

      Req.Test.stub(Client, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        decoded = Jason.decode!(body)

        assert decoded["model"] == "test-model"
        assert decoded["input"] == "Hello world"

        Req.Test.json(conn, %{
          "data" => [%{"embedding" => embedding}]
        })
      end)

      assert {:ok, ^embedding} = Client.embed("Hello world")
    end

    test "allows model override via opts" do
      Req.Test.stub(Client, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        decoded = Jason.decode!(body)

        assert decoded["model"] == "custom-model"

        Req.Test.json(conn, %{
          "data" => [%{"embedding" => [0.1, 0.2]}]
        })
      end)

      assert {:ok, [0.1, 0.2]} = Client.embed("test", model: "custom-model")
    end

    test "uses an explicit config without changing stored embedding settings" do
      Req.Test.stub(Client, fn conn ->
        assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer shadow-key"]
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        assert Jason.decode!(body)["model"] == "shadow-model"

        Req.Test.json(conn, %{
          "data" => [%{"embedding" => [0.1, 0.2]}]
        })
      end)

      config = %{
        endpoint: "http://shadow-provider",
        api_key: "shadow-key",
        model: "shadow-model"
      }

      assert {:ok, [0.1, 0.2]} = Client.embed("test", config: config)
      assert Client.model() == "test-model"
    end

    test "returns a sanitized error for an explicit endpoint without a URL scheme" do
      previous_req_options = Application.get_env(:zaq, Client, [])
      Application.put_env(:zaq, Client, req_options: [])

      on_exit(fn -> Application.put_env(:zaq, Client, previous_req_options) end)

      sensitive_endpoint = "provider-sensitive-host.invalid"

      config = %{
        endpoint: sensitive_endpoint,
        api_key: "shadow-key",
        model: "shadow-model"
      }

      log =
        capture_log(fn ->
          assert {:error, :embedding_transport_error} = Client.embed("test", config: config)
        end)

      refute log =~ sensitive_endpoint
    end

    test "rejects malformed scalar embedding data" do
      malformed_data = [
        [],
        [%{"index" => 0, "embedding" => "invalid"}],
        [%{"index" => 0, "embedding" => []}],
        [%{"index" => 0, "embedding" => [0.1, "invalid"]}],
        [%{"index" => 1, "embedding" => [0.1]}],
        [
          %{"index" => 0, "embedding" => [0.1]},
          %{"index" => 1, "embedding" => [0.2]}
        ]
      ]

      for data <- malformed_data do
        Req.Test.stub(Client, fn conn -> Req.Test.json(conn, %{"data" => data}) end)
        assert {:error, :invalid_embedding_response} = Client.embed("test")
      end
    end

    test "returns a sanitized error on unexpected response format" do
      Req.Test.stub(Client, fn conn ->
        Req.Test.json(conn, %{"unexpected" => "provider-echoed-input"})
      end)

      assert {:error, :invalid_embedding_response} = Client.embed("test")
    end

    test "returns a structured error on non-200 status" do
      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_status(401)
        |> Req.Test.json(%{"error" => "unauthorized"})
      end)

      assert {:error, {:embedding_http_error, 401}} = Client.embed("test")
    end

    test "does not expose provider response bodies in HTTP errors or logs" do
      sensitive_marker = "provider-echoed-sensitive-input"

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_status(503)
        |> Req.Test.json(%{"error" => sensitive_marker})
      end)

      log =
        capture_log(fn ->
          assert {:error, {:embedding_http_error, 503}} = Client.embed("request content")
        end)

      refute log =~ sensitive_marker
    end

    test "does not expose rate-limit response bodies in errors or logs" do
      sensitive_marker = "provider-echoed-rate-limit-input"

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "0")
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => sensitive_marker})
      end)

      log =
        capture_log(fn ->
          assert {:error, {:rate_limited, 0, %{status: 429}}} =
                   Client.embed("request content")
        end)

      refute log =~ sensitive_marker
    end

    test "returns rate_limited error with retry-after delay" do
      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "120")
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 120, %{status: 429}}} = Client.embed("test")
    end

    test "returns rate_limited error with retry-after HTTP-date delay" do
      retry_after =
        DateTime.utc_now()
        |> DateTime.add(75, :second)
        |> Calendar.strftime("%a, %d %b %Y %H:%M:%S GMT")

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", retry_after)
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, delay_seconds, %{status: 429}}} = Client.embed("test")
      assert delay_seconds >= 60
      assert delay_seconds <= 75
    end

    test "falls back to rate limit reset header when retry-after is missing" do
      reset_at = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.+(90)

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("x-ratelimit-reset", Integer.to_string(reset_at))
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, delay_seconds, %{status: 429}}} = Client.embed("test")
      assert delay_seconds >= 75
      assert delay_seconds <= 90
    end

    test "handles ratelimit-reset epoch timestamp" do
      reset_at = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.+(120)

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("ratelimit-reset", Integer.to_string(reset_at))
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, delay_seconds, %{status: 429}}} = Client.embed("test")
      assert delay_seconds >= 105
      assert delay_seconds <= 120
    end

    test "defaults to 60 seconds when no rate limit headers are present" do
      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 60, %{status: 429}}} = Client.embed("test")
    end

    test "defaults to 60 seconds when retry-after is invalid" do
      for invalid_value <- ["not-a-date", "abc", "1.5"] do
        Req.Test.stub(Client, fn conn ->
          conn
          |> Plug.Conn.put_resp_header("retry-after", invalid_value)
          |> Plug.Conn.put_status(429)
          |> Req.Test.json(%{"error" => "rate limited"})
        end)

        assert {:error, {:rate_limited, 60, %{status: 429}}} = Client.embed("test")
      end
    end

    test "defaults to 60 seconds when retry-after is negative" do
      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "-1")
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 60, %{status: 429}}} = Client.embed("test")
    end

    test "defaults to 60 seconds when retry-after is empty" do
      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "")
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 60, %{status: 429}}} = Client.embed("test")
    end

    test "uses reset header when retry-after is malformed" do
      reset_at = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.+(90)

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "abc")
        |> Plug.Conn.put_resp_header("x-ratelimit-reset", Integer.to_string(reset_at))
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, delay_seconds, %{status: 429}}} = Client.embed("test")
      assert delay_seconds >= 75
      assert delay_seconds <= 90
    end

    test "returns 0 delay for a stale reset epoch" do
      reset_at = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.-(30)

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("x-ratelimit-reset", Integer.to_string(reset_at))
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 0, %{status: 429}}} = Client.embed("test")
    end

    test "returns 0 delay when retry-after HTTP-date is in the past" do
      retry_after =
        DateTime.utc_now()
        |> DateTime.add(-30, :second)
        |> Calendar.strftime("%a, %d %b %Y %H:%M:%S GMT")

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", retry_after)
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 0, %{status: 429}}} = Client.embed("test")
    end

    test "prioritizes retry-after over x-ratelimit-reset" do
      reset_at = DateTime.utc_now() |> DateTime.to_unix() |> Kernel.+(300)

      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "7")
        |> Plug.Conn.put_resp_header("x-ratelimit-reset", Integer.to_string(reset_at))
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 7, %{status: 429}}} = Client.embed("test")
    end

    test "defaults to 60 seconds when retry-after parses as :bad_date" do
      Req.Test.stub(Client, fn conn ->
        conn
        |> Plug.Conn.put_resp_header("retry-after", "Sun, 06 Nov 94 08:49:37 GMT")
        |> Plug.Conn.put_status(429)
        |> Req.Test.json(%{"error" => "rate limited"})
      end)

      assert {:error, {:rate_limited, 60, %{status: 429}}} = Client.embed("test")
    end

    test "returns HTTP request failed when transport fails" do
      prev_req_opts = Application.get_env(:zaq, Client, [])

      on_exit(fn ->
        Application.put_env(:zaq, Client, prev_req_opts)
      end)

      Application.put_env(:zaq, Client, req_options: [])

      SystemConfigFixtures.seed_embedding_config(%{
        endpoint: unavailable_local_url(),
        api_key: "",
        model: "test-model",
        dimension: 1536
      })

      assert {:error, :embedding_transport_error} = Client.embed("test")
    end

    test "does not expose raw transport reasons in errors or logs" do
      sensitive_marker = "transport-sensitive-marker"

      Req.Test.stub(Client, fn conn ->
        Req.Test.transport_error(conn, {:bad_alpn_protocol, sensitive_marker})
      end)

      log =
        capture_log(fn ->
          assert {:error, :embedding_transport_error} = Client.embed("request content")
        end)

      refute log =~ sensitive_marker
    end

    test "handles rate-limit headers from real HTTP response map" do
      prev_req_opts = Application.get_env(:zaq, Client, [])

      on_exit(fn ->
        Application.put_env(:zaq, Client, prev_req_opts)
      end)

      Application.put_env(:zaq, Client, req_options: [])

      {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
      {:ok, port} = :inet.port(socket)
      :ok = :gen_tcp.close(socket)

      child_spec =
        {Bandit,
         plug:
           {RealHTTPStub,
            status: 429, headers: [{"retry-after", "9"}], body: %{"error" => "rate limited"}},
         scheme: :http,
         port: port}

      start_supervised!(child_spec)

      endpoint = "http://127.0.0.1:#{port}"

      SystemConfigFixtures.seed_embedding_config(%{
        endpoint: endpoint,
        api_key: "",
        model: "test-model",
        dimension: 1536
      })

      assert {:error, {:rate_limited, 9, %{status: 429}}} = Client.embed("test")
    end

    test "skips authorization header when api_key is empty" do
      Req.Test.stub(Client, fn conn ->
        auth_header = Plug.Conn.get_req_header(conn, "authorization")
        assert auth_header == []

        Req.Test.json(conn, %{
          "data" => [%{"embedding" => [0.1]}]
        })
      end)

      assert {:ok, [0.1]} = Client.embed("test")
    end

    test "includes authorization header when api_key is set" do
      SystemConfigFixtures.seed_embedding_config(%{
        endpoint: "http://localhost",
        api_key: "sk-test-key",
        model: "test-model",
        dimension: 1536
      })

      Req.Test.stub(Client, fn conn ->
        [auth] = Plug.Conn.get_req_header(conn, "authorization")
        assert auth == "Bearer sk-test-key"

        Req.Test.json(conn, %{
          "data" => [%{"embedding" => [0.1]}]
        })
      end)

      assert {:ok, [0.1]} = Client.embed("test")
    end
  end

  describe "embed_many/2" do
    test "sends array input and reorders embeddings by response index" do
      Req.Test.stub(Client, fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        decoded = Jason.decode!(body)

        assert decoded["model"] == "shadow-model"
        assert decoded["input"] == ["first", "second"]

        Req.Test.json(conn, %{
          "data" => [
            %{"index" => 1, "embedding" => [2.0]},
            %{"index" => 0, "embedding" => [1.0]}
          ]
        })
      end)

      config = %{
        endpoint: "http://shadow-provider",
        api_key: "shadow-key",
        model: "shadow-model"
      }

      assert {:ok, [[1.0], [2.0]]} =
               Client.embed_many(["first", "second"], config: config)
    end

    test "rejects duplicate or missing response indices" do
      Req.Test.stub(Client, fn conn ->
        Req.Test.json(conn, %{
          "data" => [
            %{"index" => 0, "embedding" => [1.0]},
            %{"index" => 0, "embedding" => [2.0]}
          ]
        })
      end)

      assert {:error, :invalid_embedding_response} =
               Client.embed_many(["first", "second"])
    end

    test "rejects non-numeric embedding values" do
      Req.Test.stub(Client, fn conn ->
        Req.Test.json(conn, %{
          "data" => [%{"index" => 0, "embedding" => [1.0, "invalid"]}]
        })
      end)

      assert {:error, :invalid_embedding_response} = Client.embed_many(["first"])
    end

    test "rejects empty input without making an HTTP request" do
      Req.Test.stub(Client, fn _conn ->
        flunk("empty batch must not issue an HTTP request")
      end)

      assert {:error, "Embedding batch must not be empty"} = Client.embed_many([])
    end
  end

  defp unavailable_local_url do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    "http://127.0.0.1:#{port}"
  end
end
