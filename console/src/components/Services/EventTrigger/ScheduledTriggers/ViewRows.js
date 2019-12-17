import React from 'react';

// import { useQuery } from '@apollo/react-hooks';
import useHasuraQuery from './useHasuraQuery';

import { fetchScheduledTriggers } from './Actions';

import DragFoldTable from '../../../Common/TableCommon/DragFoldTable';

const tableScss = require('../../../Common/TableCommon/TableStyles.scss');

/*
const LIMIT = 5;

const defaultState = {
  limit: LIMIT,
  offset: 0,
  order_by: '',
};
*/
const ViewScheduledTriggerRows = props => {
  const { dispatch } = props;

  const { loading, error, data } = useHasuraQuery({
    query: fetchScheduledTriggers(),
    dispatcher: dispatch,
  });

  if (loading) {
    return <span>Fetching current operations...</span>;
  }
  if (error) {
    return (
      <span>
        Error fetching
        <code>{error.toString()}</code>
      </span>
    );
  }

  const showDataNotAvailableMessage = () => {
    return <div>There are no scheduled triggers created yet!</div>;
  };

  if (!data || (data && 'result' in data && data.result.length === 1)) {
    return showDataNotAvailableMessage();
  }

  /*
  const renderRefetchButtonText = () => {
    if (loading && checkObjectValidity(data)) {
      return 'Reloading...';
    }
    return 'Reload current operations';
  };
  */

  const info = data.result;

  const getHeaders = () => {
    if (info.length > 0) {
      const getColWidth = (header, contentRows = []) => {
        const MAX_WIDTH = 600;
        const HEADER_PADDING = 50;
        const CONTENT_PADDING = 24;
        const HEADER_FONT = 'bold 16px Gudea';
        const CONTENT_FONT = '14px Gudea';

        const getTextWidth = (text, font) => {
          // Doesn't work well with non-monospace fonts
          // const CHAR_WIDTH = 8;
          // return text.length * CHAR_WIDTH;

          // if given, use cached canvas for better performance
          // else, create new canvas
          const canvas =
            getTextWidth.canvas ||
            (getTextWidth.canvas = document.createElement('canvas'));

          const context = canvas.getContext('2d');
          context.font = font;

          const metrics = context.measureText(text);
          return metrics.width;
        };

        let maxContentWidth = 0;
        for (let i = 0; i < contentRows.length; i++) {
          if (contentRows[i] !== undefined && contentRows[i][header] !== null) {
            const content = contentRows[i][header];

            let contentString;
            if (content === null || content === undefined) {
              contentString = 'NULL';
            } else if (typeof content === 'object') {
              contentString = JSON.stringify(content, null, 4);
            } else {
              contentString = content.toString();
            }

            const currLength = getTextWidth(contentString, CONTENT_FONT);

            if (currLength > maxContentWidth) {
              maxContentWidth = currLength;
            }
          }
        }

        const maxContentCellWidth = maxContentWidth + CONTENT_PADDING;

        const headerWithUnit = () => {
          return header;
        };

        const headerCellWidth =
          getTextWidth(headerWithUnit(), HEADER_FONT) + HEADER_PADDING;

        return Math.min(
          MAX_WIDTH,
          Math.max(maxContentCellWidth, headerCellWidth)
        );
      };
      const columns = info[0];
      const headerRows = columns.map((c, key) => {
        return {
          Header: (
            <div key={key} className="ellipsis" title="Click to sort">
              {c}
            </div>
          ),
          accessor: c,
          id: c,
          foldable: true,
          width: getColWidth(c, info),
        };
      });
      const actionRow = {
        Header: <div key={'action_operation_header'}>Actions</div>,
        accessor: 'tableRowActionButtons',
        id: 'tableRowActionButtons',
        width: 100,
      };
      return [actionRow, ...headerRows];
    }
    return [];
  };

  /*
  const renderActionButtonForGroups = name => {
    return (
      <DeleteFromOperationGroup
        projectId={projectId}
        operationGroupName={collectionName}
        operationName={name}
        refetch={refetch}
        dispatch={dispatch}
      />
    );
  };
  */

  const getRows = () => {
    if (info.length > 1) {
      const dat = info.slice(1);
      return dat.map(d => {
        const newRow = {};
        newRow.tableRowActionButtons = <div>Cancel</div>;
        d.forEach((elem, key) => {
          newRow[info[0][key]] = (
            <div key={key} title={elem}>
              {elem}
            </div>
          );
        });
        return newRow;
      });
    }
    return [];
  };

  const _rows = getRows();
  const _columns = getHeaders();

  const renderOperationTable = () => {
    if (info.length > 1) {
      /*
      const renderImportAllowList = () => {
        return (
          <ImportAllowList
            name={collectionName}
            projectId={projectId}
          />
        );
      };
      */
      return (
        <div className={tableScss.tableContainer}>
          <DragFoldTable
            className="-highlight -fit-content"
            data={_rows}
            columns={_columns}
            resizable
            manual
            showPagination={false}
            sortable={false}
            minRows={0}
          />
        </div>
      );
    }
    return showDataNotAvailableMessage();
  };

  return (
    <div className={`row ${tableScss.add_mar_top}`}>
      <div className="col-xs-12">
        <div>{renderOperationTable()}</div>
      </div>
    </div>
  );
};

export default ViewScheduledTriggerRows;