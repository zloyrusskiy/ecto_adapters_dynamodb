defmodule Ecto.Adapters.DynamoDB.Storage do
    defmacro __using__(_opts) do
        quote do
            @behaviour Ecto.Adapter.Storage

            @impl Ecto.Adapter.Storage
            def storage_down(_options) do
                :ok
            end

            @impl Ecto.Adapter.Storage
            def storage_status(_options) do
                # DynamoDB does not require database creation
                :up
            end

            @impl Ecto.Adapter.Storage
            def storage_up(_options) do
                :ok
            end
        end
    end
end