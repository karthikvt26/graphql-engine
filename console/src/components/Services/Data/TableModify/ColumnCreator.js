import React, { useState } from 'react';
import { showErrorNotification } from '../Notification';
import gqlPattern, { gqlColumnErrorNotif } from '../Common/GraphQLValidation';
import dataTypes from '../Common/DataTypes';

import SearchableSelectBox from '../../../Common/SearchableSelect/SearchableSelect';

import { getDataOptions } from '../Add/utils';

import Button from '../../../Common/Button/Button';
import { addColSql } from '../TableModify/ModifyActions';

import styles from './ModifyTable.scss';

const useColumnEditor = (dispatch, tableName) => {
  const initialState = {
    colName: '',
    colType: '',
    colNull: true,
    colUnique: false,
    colDefault: '',
  };

  const [columnState, setColumnState] = useState(initialState);
  const { colName, colType, colNull, colUnique, colDefault } = columnState;

  const onSubmit = e => {
    e.preventDefault();

    // validate before sending
    if (!gqlPattern.test(colName)) {
      dispatch(
        showErrorNotification(
          gqlColumnErrorNotif[0],
          gqlColumnErrorNotif[1],
          gqlColumnErrorNotif[2],
          gqlColumnErrorNotif[3]
        )
      );
    } else if (colName === '' || colType === '') {
      dispatch(
        showErrorNotification(
          'Error creating column!',
          'Column name/type cannot be empty',
          '',
          {
            custom: 'Column name/type cannot be empty',
          }
        )
      );
    } else {
      dispatch(
        addColSql(
          tableName,
          colName,
          colType,
          colNull,
          colUnique,
          colDefault,
          () => setColumnState(initialState)
        )
      );
    }
  };

  return {
    colName: {
      value: colName,
      onChange: e => {
        setColumnState({ ...columnState, colName: e.target.value });
      },
    },
    colType: {
      value: colType,
      onChange: selected => {
        setColumnState({ ...columnState, colType: selected.value });
      },
    },
    colNull: {
      checked: colNull,
      onChange: e => {
        setColumnState({ ...columnState, colNull: e.target.checked });
      },
    },
    colUnique: {
      checked: colUnique,
      onChange: e => {
        setColumnState({ ...columnState, colUnique: e.target.checked });
      },
    },
    colDefault: {
      value: colDefault,
      onChange: e => {
        setColumnState({ ...columnState, colDefault: e.target.value });
      },
    },
    onSubmit,
  };
};

/*
const alterTypeOptions = dataTypes.map((datatype, index) => (
  <option value={datatype.value} key={index} title={datatype.description}>
    {datatype.name}
  </option>
));
*/

const ColumnCreator = ({ dispatch, tableName, dataTypes: restTypes = [] }) => {
  const {
    colName,
    colType,
    colNull,
    colUnique,
    colDefault,
    onSubmit,
  } = useColumnEditor(dispatch, tableName);

  const { columnDataTypes, columnTypeValueMap } = getDataOptions(
    dataTypes,
    restTypes,
    0
  );

  const customStyles = {
    container: provided => ({
      ...provided,
      width: '186px',
    }),
    dropdownIndicator: provided => {
      return {
        ...provided,
        padding: '5px',
      };
    },
    placeholder: provided => {
      return {
        ...provided,
        top: '44%',
        fontSize: '12px',
      };
    },
    singleValue: provided => {
      return {
        ...provided,
        fontSize: '12px',
        top: '44%',
        color: '#555555',
      };
    },
  };

  return (
    <div className={styles.activeEdit}>
      <form
        className={`form-inline ${styles.display_flex}`}
        onSubmit={onSubmit}
      >
        <input
          placeholder="column name"
          type="text"
          className={`${styles.input} input-sm form-control`}
          data-test="column-name"
          {...colName}
        />
        <span className={`${styles.select}`} data-test="col-type-0">
          <SearchableSelectBox
            options={columnDataTypes}
            onChange={colType.onChange}
            value={colType.value && columnTypeValueMap[colType.value]}
            bsClass={`col-type-${0} modify_select`}
            customStyle={customStyles}
          />
        </span>
        {/*
        <select
          className={`${styles.select} input-sm form-control`}
          data-test="data-type"
          {...colType}
        >
          <option disabled value="">
            -- type --
          </option>
          {alterTypeOptions}
        </select>
        */}

        <input
          type="checkbox"
          className={`${styles.input} ${styles.nullable} input-sm form-control`}
          data-test="nullable-checkbox"
          {...colNull}
        />
        <label className={styles.nullLabel}>Nullable</label>

        <input
          type="checkbox"
          className={`${styles.input} ${styles.nullable} input-sm form-control`}
          {...colUnique}
          data-test="unique-checkbox"
        />
        <label className={styles.nullLabel}>Unique</label>

        <input
          placeholder="default value"
          type="text"
          className={`${styles.input} ${
            styles.defaultInput
          } input-sm form-control`}
          {...colDefault}
          data-test="default-value"
        />

        <Button
          type="submit"
          color="yellow"
          size="sm"
          data-test="add-column-button"
        >
          + Add column
        </Button>
      </form>
    </div>
  );
};

export default ColumnCreator;
