protoc --proto_path=./pb \
        --go_out=paths=source_relative:./pb \
        ./pb/source_data.proto
