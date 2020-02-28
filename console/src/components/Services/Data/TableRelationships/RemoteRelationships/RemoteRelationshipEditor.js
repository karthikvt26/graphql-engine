import React from 'react';
import ExpandableEditor from '../../../../Common/Layout/ExpandableEditor/Editor';
import RemoteRelationshipExplorer from './GraphQLSchemaExplorer';
import styles from '../../TableModify/ModifyTable.scss';
import {
  relName as relNameTooltip,
  remoteSchema as remoteSchemaTooltip,
  configuration as configTooltip,
} from './Tooltips';
import { getRemoteRelConfig } from '../utils';
import {
  setRemoteRelationships,
  defaultRemoteRelationship,
  saveRemoteRelationship,
  dropRemoteRelationship,
} from '../Actions';

const RemoteRelationshipEditor = ({
  relationship,
  dispatch,
  allRelationships,
  index,
  numRels,
  existingRelationship,
  tableSchema,
  remoteSchemas,
  adminHeaders,
}) => {
  const isLast = index === numRels - 1;

  // handle relationship name change
  const handleRelnameChange = e => {
    const newRelationships = JSON.parse(JSON.stringify(allRelationships));
    newRelationships[index].name = e.target.value;
    dispatch(setRemoteRelationships(newRelationships));
  };

  // handle remote schema selection change
  const handleRemoteSchemaChange = e => {
    const newRelationships = JSON.parse(JSON.stringify(allRelationships));
    if (e.target.value === newRelationships[index].remoteSchema) {
      return;
    }
    const relName = newRelationships[index].name;
    newRelationships[index] = JSON.parse(
      JSON.stringify(defaultRemoteRelationship)
    );
    newRelationships[index].name = relName;
    newRelationships[index].remoteSchema = e.target.value;
    dispatch(setRemoteRelationships(newRelationships));
  };

  // handle remote field change
  const handleRemoteFieldChange = (fieldName, nesting, checked) => {
    const newRelationships = JSON.parse(JSON.stringify(allRelationships));
    const newRemoteField = newRelationships[index].remoteField.filter(
      rf => rf.name !== fieldName && rf.nesting < nesting
    );
    if (checked) {
      newRemoteField.push({
        name: fieldName,
        nesting,
        arguments: [],
      });
    }
    newRelationships[index].remoteField = newRemoteField;
    dispatch(setRemoteRelationships(newRelationships));
  };

  // handle argument change
  const handleArgChange = (
    fieldName,
    nesting,
    argName,
    argNesting,
    checked,
    parentArg
  ) => {
    const newRelationships = JSON.parse(JSON.stringify(allRelationships));
    const concernedRemoteField = newRelationships[index].remoteField.find(
      rf => rf.name === fieldName && nesting === rf.nesting
    );
    let concernedArgs = concernedRemoteField.arguments.filter(a => {
      return !(
        a.name === argName &&
        a.argNesting === argNesting &&
        a.parentArg === parentArg
      );
    });
    const removeChildrenArgs = p => {
      const childrenArgList = [];
      concernedArgs = concernedArgs.filter(ca => {
        if (ca.parentArg === p) {
          childrenArgList.push(ca.name);
          return false;
        }
        return true;
      });
      childrenArgList.forEach(ca => removeChildrenArgs(`${p}.${ca}`));
    };
    removeChildrenArgs(argName);
    if (checked) {
      concernedArgs.push({
        name: argName,
        argNesting,
        parentArg,
      });
    }
    concernedRemoteField.arguments = concernedArgs;
    newRelationships[index].remoteField = newRelationships[
      index
    ].remoteField.map(rf => {
      if (rf.name === fieldName && rf.nesting === nesting) {
        return concernedRemoteField;
      }
      return rf;
    });
    dispatch(setRemoteRelationships(newRelationships));
  };

  // handle argument value change
  const handleArgValueChange = (
    value,
    isColumn,
    fieldName,
    fieldNesting,
    arg
  ) => {
    const newRelationships = JSON.parse(JSON.stringify(allRelationships));
    const concernedRemoteField = newRelationships[index].remoteField.find(
      rf => rf.name === fieldName && fieldNesting === rf.nesting
    );
    const concernedArgs = concernedRemoteField.arguments.filter(a => {
      return !(
        a.name === arg.name &&
        a.argNesting === arg.argNesting &&
        a.parentArg === arg.parentArg
      );
    });
    concernedArgs.push({
      name: arg.name,
      parentArg: arg.parentArg,
      argNesting: arg.argNesting,
      static: undefined,
      column: undefined,
      [isColumn ? 'column' : 'static']: value,
    });
    concernedRemoteField.arguments = concernedArgs;
    newRelationships[index].remoteField = newRelationships[
      index
    ].remoteField.map(rf => {
      if (rf.name === fieldName && rf.nesting === fieldNesting) {
        return concernedRemoteField;
      }
      return rf;
    });
    dispatch(setRemoteRelationships(newRelationships));
  };

  // render relationship name textbox
  const relNameTextBox = () => {
    let title;
    if (!isLast) {
      title =
        'Relationship name cannot be changed. Please drop and recreate the relationship if you wish to rename.';
    }
    return (
      <div>
        <div className={`${styles.add_mar_bottom}`}>
          <div
            className={`${styles.add_mar_bottom_mid} ${styles.display_flex}`}
          >
            <div className={styles.add_mar_right_small}>
              <b>Name</b>
            </div>
            <div>{relNameTooltip(tableSchema.table_name)}</div>
          </div>
          <div>
            <input
              type="text"
              className={`form-control ${styles.wd300Px}`}
              placeholder="name"
              value={relationship.name}
              onChange={handleRelnameChange}
              disabled={!isLast}
              title={title}
            />
          </div>
        </div>
      </div>
    );
  };

  // render remote schema select
  const remoteSchemaSelect = () => {
    const placeHolder = !relationship.remoteSchema && (
      <option key="placeholder" value="">
        {' '}
        -- remote schema --
      </option>
    );
    const remoteSchemaOptions = remoteSchemas.map(s => {
      return (
        <option key={s} value={s}>
          {s}
        </option>
      );
    });
    return (
      <div>
        <div className={`${styles.add_mar_bottom}`}>
          <div
            className={`${styles.add_mar_bottom_mid} ${styles.display_flex} ${styles.add_mar_right_small}`}
          >
            <div className={styles.add_mar_right_small}>
              <b>Remote Schema:</b>
            </div>
            <div>{remoteSchemaTooltip(tableSchema.table_name)}</div>
          </div>
          <div>
            <select
              className={`form-control ${styles.wd300Px}`}
              value={relationship.remoteSchema}
              onChange={handleRemoteSchemaChange}
            >
              {placeHolder}
              {remoteSchemaOptions}
            </select>
          </div>
        </div>
      </div>
    );
  };

  const relationshipExplorer = () => (
    <RemoteRelationshipExplorer
      dispatch={dispatch}
      relationship={relationship}
      handleArgChange={handleArgChange}
      handleRemoteFieldChange={handleRemoteFieldChange}
      handleArgValueChange={handleArgValueChange}
      tableSchema={tableSchema}
      adminHeaders={adminHeaders}
    />
  );

  const expandedContent = () => {
    return (
      <div>
        {relNameTextBox()}
        {remoteSchemaSelect()}
        <div>
          <div
            className={`${styles.add_mar_bottom_mid} ${styles.display_flex} ${styles.add_mar_right_small}`}
          >
            <div className={styles.add_mar_right_small}>
              <b>Configuration:</b>
            </div>
            <div>{configTooltip()}</div>
          </div>
          {relationshipExplorer()}
        </div>
      </div>
    );
  };

  const saveFunc = toggle => {
    const successCallback = () => {
      if (isLast) {
        toggle();
        const newRelationships = JSON.parse(JSON.stringify(allRelationships));
        newRelationships.push({
          ...defaultRemoteRelationship,
        });
        dispatch(setRemoteRelationships(newRelationships));
      }
    };
    dispatch(saveRemoteRelationship(index, isLast, successCallback));
  };

  let removeFunc;
  if (!isLast) {
    removeFunc = () => {
      const isOk = window.confirm('Are you sure?');
      if (!isOk) return;
      const successCallback = () => {
        const newRelationships = JSON.parse(JSON.stringify(allRelationships));
        dispatch(
          setRemoteRelationships([
            ...newRelationships.slice(0, index),
            ...newRelationships.slice(index + 1),
          ])
        );
      };
      dispatch(dropRemoteRelationship(index, successCallback));
    };
  }

  const expandButtonText = isLast
    ? numRels > 1
      ? 'Add a new remote relationship'
      : 'Add a remote relationship'
    : 'Edit';
  const collapseButtonText = isLast ? 'Cancel' : 'Close';

  const collapsedLabel = () =>
    getRemoteRelConfig(relationship, tableSchema.table_name, styles);

  const collapseCallback = () => {
    if (isLast) return;
    const newRelationships = JSON.parse(JSON.stringify(allRelationships));
    dispatch(
      setRemoteRelationships([
        ...newRelationships.slice(0, index),
        existingRelationship,
        ...newRelationships.slice(index + 1),
      ])
    );
  };

  return (
    <ExpandableEditor
      editorExpanded={expandedContent}
      property={'remote-relationship-add'}
      service="table-relationship"
      saveFunc={saveFunc}
      expandButtonText={expandButtonText}
      collapseButtonText={collapseButtonText}
      collapseCallback={collapseCallback}
      collapsedLabel={collapsedLabel}
      removeFunc={removeFunc}
    />
  );
};

export default RemoteRelationshipEditor;
