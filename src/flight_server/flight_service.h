/* flight_service.h — Arrow Flight service that bridges to the DataFlow OS
 * gateway over HTTP. Implements a minimal subset of the Flight protocol:
 *
 *   ListFlights     — enumerate tables as flights
 *   GetFlightInfo   — schema of a specific flight (table or query)
 *   DoGet           — execute a SQL query, stream Arrow record batches
 *   DoPut           — TODO; currently returns Status::NotImplemented
 *   DoExchange      — not implemented
 *
 * Supported Ticket payload formats:
 *   "sql:SELECT …"      — execute SQL via /api/tables/query
 *   "table:my_table"    — equivalent to "sql:SELECT * FROM my_table"
 */
#pragma once
#include <arrow/flight/server.h>
#include <memory>
#include <string>

#include "http_client.h"

namespace dfo {

class DfoFlightService : public arrow::flight::FlightServerBase {
public:
    DfoFlightService(std::string gateway_url, std::string api_key);
    ~DfoFlightService() override;

    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::Criteria* criteria,
        std::unique_ptr<arrow::flight::FlightListing>* listings) override;

    arrow::Status GetFlightInfo(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::FlightDescriptor& descriptor,
        std::unique_ptr<arrow::flight::FlightInfo>* info) override;

    arrow::Status DoGet(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::Ticket& request,
        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

    arrow::Status DoPut(
        const arrow::flight::ServerCallContext& ctx,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

private:
    std::unique_ptr<HttpClient> http_;
    std::string                 gateway_url_;
    std::string                 api_key_;
};

}  // namespace dfo
