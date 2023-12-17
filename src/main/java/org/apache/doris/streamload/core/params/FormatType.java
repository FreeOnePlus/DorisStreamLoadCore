package org.apache.doris.streamload.core.params;

import lombok.Getter;

@Getter
public enum FormatType {
    csv,
    csv_with_names,
    csv_with_names_and_types,
    json
}
