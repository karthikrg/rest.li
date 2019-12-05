/*
   Copyright (c) 2019 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.linkedin.data.schema.annotation;

import com.linkedin.data.message.Message;
import com.linkedin.data.message.MessageList;
import com.linkedin.data.message.MessageUtil;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaConstants;
import com.linkedin.data.schema.DataSchemaTraverse;
import com.linkedin.data.schema.MapDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.data.schema.UnionDataSchema;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Expanded from {@link com.linkedin.data.schema.DataSchemaTraverse}
 * There are two main differences:
 * (1) This new traverser provides rich context that passed to the visitors when visiting data schemas
 * (2) It will also traverse to the schemas even the schema has been seen before. (But there are mechanisms to prevent cycles)
 */
public class DataSchemaRichContextTraverser
{
  /**
   * Use this {@link IdentityHashMap} to prevent traversing through a cycle, when traversing along parents to child.
   * for the example below:
   * <pre>
   * record Rcd{
   *   f1: Rcd
   * }
   * </pre>
   *
   * This hashmap help the traverser to recognize when encountering "Rcd" for the second time.
   */
  private final IdentityHashMap<DataSchema, Boolean> _seenDataSchema = new IdentityHashMap<>();
  private SchemaVisitor _schemaVisitor;
  /**
   * Store the original data schema that has been passed.
   * The {@link DataSchemaRichContextTraverser} should no modify this DataSchema during the traversal
   * which could ensure the correctness of the traversal
   *
   */
  private DataSchema _originalDataSchemaUnderTraversal;


  public DataSchemaRichContextTraverser(SchemaVisitor schemaVisitor)
  {
    _schemaVisitor = schemaVisitor;
  }

  public void traverse(DataSchema schema)
  {
    _originalDataSchemaUnderTraversal = schema;
    TraverserContext traverserContext = new TraverserContext();
    traverserContext.setCurrentSchema(schema);
    traverserContext.setVisitorContext(_schemaVisitor.getInitialVisitorContext());
    onRecursion(traverserContext);

  }

  private void onRecursion(TraverserContext context)
  {

    // Add full name to the context's TraversePath
    // For NamedDataSchema, it would return Full Name
    DataSchema schema = context.getCurrentSchema();
    ArrayDeque<String> path = context.getTraversePath();
    path.add(schema.getUnionMemberKey());

    // visitors
    _schemaVisitor.callbackOnContext(context, DataSchemaTraverse.Order.PRE_ORDER);

    /**
     * By default {@link DataSchemaRichContextTraverser} will only decide whether or not keep traversing based on whether the new
     * data schema has been seen.
     *
     * But the {@link SchemaVisitor} has the chance to override this control by setting {@link TraverserContext#shouldContinue}
     */
    if (context.shouldContinue() == Boolean.TRUE || !(context.shouldContinue() == Boolean.FALSE || _seenDataSchema.containsKey(schema)) )
    {
      _seenDataSchema.put(schema, Boolean.TRUE);

      //Pass new context in every recursion
      TraverserContext nextContext = context.getNextContext();

      switch (schema.getType())
      {
        case TYPEREF:
          TyperefDataSchema typerefDataSchema = (TyperefDataSchema) schema;
          nextContext.getTraversePath().add(DataSchemaConstants.REF_KEY);
          nextContext.setCurrentSchema(typerefDataSchema.getRef());
          nextContext.setCurrentSchemaEntryMode(CurrentSchemaEntryMode.TYPEREF_REF);
          // Set other few
          onRecursion(nextContext);
          break;
        case MAP:
          //traverse key if has matched
          MapDataSchema mapDataSchema = (MapDataSchema) schema;
          nextContext.getTraversePath().add(DataSchemaConstants.MAP_KEY_REF);
          nextContext.getSchemaPathSpec().add(DataSchemaConstants.MAP_KEY_REF);
          nextContext.setCurrentSchema(mapDataSchema.getKey());
          nextContext.setCurrentSchemaEntryMode(CurrentSchemaEntryMode.MAP_KEY);

          onRecursion(nextContext);

          //then traverse values
          nextContext.setTraversePath(new ArrayDeque<>(path));
          nextContext.getTraversePath().add(PathSpec.WILDCARD);
          nextContext.setSchemaPathSpec(new ArrayDeque<>(context.getSchemaPathSpec()));
          nextContext.getSchemaPathSpec().add(PathSpec.WILDCARD);

          nextContext.setCurrentSchema(mapDataSchema.getValues());
          nextContext.setCurrentSchemaEntryMode(CurrentSchemaEntryMode.MAP_VALUE);

          nextContext.setVisitorContext(context.getVisitorContext());

          onRecursion(nextContext);
          break;
        case ARRAY:
          ArrayDataSchema arrayDataSchema = (ArrayDataSchema) schema;
          nextContext.getTraversePath().add(PathSpec.WILDCARD);
          nextContext.getSchemaPathSpec().add(PathSpec.WILDCARD);
          nextContext.setCurrentSchema(arrayDataSchema.getItems());
          nextContext.setCurrentSchemaEntryMode(CurrentSchemaEntryMode.ARRAY_VALUE);

          onRecursion(nextContext);
          break;
        case RECORD:
          RecordDataSchema recordDataSchema = (RecordDataSchema) schema;
          for (RecordDataSchema.Field field : recordDataSchema.getFields())
          {
            nextContext.setTraversePath(new ArrayDeque<>(path));
            nextContext.getTraversePath().add(field.getName());
            nextContext.setSchemaPathSpec(new ArrayDeque<>(context.getSchemaPathSpec()));
            nextContext.getSchemaPathSpec().add(field.getName());

            nextContext.setCurrentSchema(field.getType());
            nextContext.setCurrentSchemaEntryMode(CurrentSchemaEntryMode.FIELD);
            nextContext.setEnclosingField(field);

            nextContext.setVisitorContext(context.getVisitorContext());

            onRecursion(nextContext);
          }
          break;
        case UNION:
          UnionDataSchema unionDataSchema = (UnionDataSchema) schema;
          for (UnionDataSchema.Member member : unionDataSchema.getMembers())
          {
            nextContext.setTraversePath(new ArrayDeque<>(path));
            nextContext.getTraversePath().add(member.getUnionMemberKey());
            nextContext.setSchemaPathSpec(new ArrayDeque<>(context.getSchemaPathSpec()));
            nextContext.getSchemaPathSpec().add(member.getUnionMemberKey());

            nextContext.setCurrentSchema(member.getType());
            nextContext.setCurrentSchemaEntryMode(CurrentSchemaEntryMode.UNION_MEMBER);
            nextContext.setEnclosingUnionMember(member);

            nextContext.setVisitorContext(context.getVisitorContext());

            onRecursion(nextContext);
          }
          break;
        case FIXED:
          // treated similar to Primitive
        case ENUM:
          // treated similar to Primitive
          break;
        default:
          assert (schema.isPrimitive());
          break;
      }
      _seenDataSchema.remove(schema);
    }
    _schemaVisitor.callbackOnContext(context, DataSchemaTraverse.Order.POST_ORDER);
  }

  /**
   * Enum to tell how the current schema is linked from its parentSchema as a child schema
   */
  enum CurrentSchemaEntryMode
  {
    // child schema is a record field
    FIELD,
    // child schema is key field of map
    MAP_KEY,
    // child schema is value field of map
    MAP_VALUE,
    // child schema is value field of array
    ARRAY_VALUE,
    // child schema is a member of union
    UNION_MEMBER,
    // child schema is referred from a typeref schema
    TYPEREF_REF
  }

  /**
   * Interface for SchemaVisitor, which will be called by {@link DataSchemaRichContextTraverser}
   */
  public interface SchemaVisitor
  {
    /**
     * the callback function that will be called by {@link DataSchemaRichContextTraverser} visiting the dataSchema under traversal
     * this function will be called TWICE within {@link DataSchemaRichContextTraverser}, during two {@link DataSchemaTraverse.Order}s
     * {@link DataSchemaTraverse.Order#PRE_ORDER} and {@link DataSchemaTraverse.Order#POST_ORDER} respectively
     *
     * @param context
     * @param order the order given by {@link DataSchemaRichContextTraverser} to tell whether this call happens during pre order or post order
     */
    void callbackOnContext(TraverserContext context, DataSchemaTraverse.Order order);

    /**
     * {@link SchemaVisitor} implements this method to return a initial {@link VisitorContext}
     * {@link VisitorContext} will be stored inside {@link TraverserContext} and then passed to {@link SchemaVisitor} during recursive traversal
     *
     * @return a initial {@link VisitorContext} defined and initialized by {@link SchemaVisitor}
     *
     * also see {@link VisitorContext}
     *
     */
    VisitorContext getInitialVisitorContext();

    /**
     * The visitor should store a {@link VisitorTraversalResult} which stores this visitor's traversal status and result.
     *
     * @return traversal result after the visitor traversed the schema
     */
    VisitorTraversalResult getVisitorTraversalResult();

    /**
     * The {@link SchemaVisitor} implementation can construct a new {@link DataSchema} after visiting the traversal.
     * The {@link SchemaVisitor} should not mutate the original {@link DataSchema} that {@link DataSchemaRichContextTraverser} is traversing.
     *
     * This is useful if a new {@link DataSchema} needs to be produced for later use
     *
     * @return a schema if the visitor is constructing one, otherwise return null;
     */
    default DataSchema getConstructedSchema()
    {
      return null;
    }
  }

  /**
   * A context that is defined and handled by {@link SchemaVisitor} themselves
   *
   * The {@link DataSchemaRichContextTraverser} will get the initial context and then
   * passing this as part of {@link TraverserContext}
   *
   * {@link SchemaVisitor} implementations can store customized information that want to pass during recursive traversal here
   * similar to how {@link TraverserContext} is used.
   *
   * also see {@link TraverserContext}
   *
   */
  public interface VisitorContext
  {
  }

  /**
   * The traversal result stores states of the traversal result
   * It should tell whether the traversal is successful and stores error messages if not
   *
   * There are two kinds of error messages
   * (1) An error message with {@link Message} type, it will be collected to the {@link Message} list and formatted and
   * outputted by the string builder, also see {@link Message}.
   * (2) Since all {@link Message}s will be finally formatted and outputted by a string builder. User can also directly add
   * string literal messages and they will be outputted by that string builder.
   *
   */
  public static class VisitorTraversalResult
  {
    /**
     * Return whether there are errors detected during the traversal
     * @return boolean to tell whether the traversal is successful or not
     *
     */
    public boolean isTraversalSuccessful()
    {
      return _isTraversalSuccessful;
    }

    /**
     * Set whether the traversal is successful
     *
     * also see {@link #isTraversalSuccessful()}
     * @param traversalSuccessful
     */
    public void setTraversalSuccessful(boolean traversalSuccessful)
    {
      _isTraversalSuccessful = traversalSuccessful;
    }

    /**
     * Getter for messages lists
     * @return collection of messages gather during traversal
     */
    public Collection<Message> getMessages()
    {
      return _messages;
    }

    /**
     * Setter for message lists
     * @param messages
     */
    public void setMessages(MessageList<Message> messages)
    {
      _messages = messages;
    }

    /**
     * Add a message to the message list and the string builder
     * @param message
     */
    public void addMessage(Message message)
    {
      _messages.add(message);
      MessageUtil.appendMessages(getMessageBuilder(), Arrays.asList(message));
      setTraversalSuccessful(false);
    }

    /**
     * Add a {@link Message} to the message list using constructor of the {@link Message}
     * and also add to the string builder
     *
     * @param path path to show in the message
     * @param format format of the message to show
     * @param args args for the format string
     *
     * also see {@link Message}
     */
    public void addMessage(ArrayDeque<String> path, String format, Object... args)
    {
      Message msg = new Message(path.toArray(), format, args);
      _messages.add(msg);
      MessageUtil.appendMessages(getMessageBuilder(), Arrays.asList(msg));
      setTraversalSuccessful(false);
    }

    /**
     * add multiple {@link Message}s to the message list and the string builder
     * These message added shows same path
     *
     * @param path path of the location where the messages are added
     * @param messages the message to add to the message list
     *
     * also see {@link Message}
     */
    public void addMessages(ArrayDeque<String> path, Collection<? extends Message> messages)
    {
      List<Message> msgs = messages.stream()
                                   .map(msg -> new Message(path.toArray(), ((Message) msg).toString()))
                                   .collect(Collectors.toList());
      _messages.addAll(msgs);
      MessageUtil.appendMessages(getMessageBuilder(), msgs);
      setTraversalSuccessful(false);
    }

    /**
     * construct messages using literal strings and add to the message list and the string builder
     *
     * @param path path to show in the message
     * @param literalMessages the literal message content
     */
    public void addMessages(List<String> path, List<String> literalMessages)
    {
      List<Message> msgs = literalMessages.stream()
                                   .map(msg -> new Message(path.toArray(), msg))
                                   .collect(Collectors.toList());
      _messages.addAll(msgs);
      MessageUtil.appendMessages(getMessageBuilder(), msgs);
      setTraversalSuccessful(false);
    }

    /**
     * add multiple {@link Message}s to the message list and the string builder
     *
     * @param messages collection of {@link Message}s
     */
    public void addMessages(Collection<? extends Message> messages)
    {
      _messages.addAll(messages);
      MessageUtil.appendMessages(getMessageBuilder(), messages);
      setTraversalSuccessful(false);
    }

    /**
     * add string to message string builder directly
     *
     * @param message
     */
    public void addLiteralMessage(String message)
    {
      getMessageBuilder().append(message);
    }

    public StringBuilder getMessageBuilder()
    {
      return _messageBuilder;
    }

    /**
     * Output the string builder content as a string
     *
     * @return a string output by the string builder
     */
    public String formatToErrorMessage()
    {
      return getMessageBuilder().toString();
    }

    boolean _isTraversalSuccessful = true;
    MessageList<Message> _messages = new MessageList<>();
    StringBuilder _messageBuilder = new StringBuilder();
  }

  /**
   * Context defined by {@link DataSchemaRichContextTraverser} that will be passed and handled during traversal
   *
   * A new TraverserContext object will be created before entering child from parent so it simulates elements inside stack during recursive traversal
   */
  static class TraverserContext
  {


    // Use this flag to control whether continue traversing child schemas
    Boolean shouldContinue = null;
    DataSchema _currentSchema;

    /**
     * return {@link Boolean} object to tell the {@link DataSchemaRichContextTraverser} whether the traversal should continue
     * If this variable set to be {@link Boolean#TRUE}, the {@link DataSchemaRichContextTraverser} will traverse to next level (if applicable)
     * If this variable set to be {@link Boolean#FALSE}, the {@link DataSchemaRichContextTraverser} will stop traversing to next level
     * If this variable not set, the {@link DataSchemaRichContextTraverser} will decide whether or not to continue traversing
     *
     * @return the {@link Boolean} variable
     */
    public Boolean shouldContinue()
    {
      return shouldContinue;
    }

    public void setShouldContinue(Boolean shouldContinue)
    {
      this.shouldContinue = shouldContinue;
    }
    /**
     * This traverse path is a very detailed path, and is same as the path used in {@link DataSchemaTraverse}
     * This path's every component corresponds to a move by traverser, and its components have TypeRef components and record name.
     * Example:
     * <pre>
     * record Test {
     *   f1: record Nested {
     *     f2: typeref TypeRef_Name=int
     *   }
     * }
     * </pre>
     * The traversePath to the f2 field would be as detailed as "/Test/f1/Nested/f2/TypeRef_Name/int"
     * Meanwhile its schema pathSpec is as simple as "/f1/f2"
     *
     */
    ArrayDeque<String> _traversePath = new ArrayDeque<>();
    /**
     * This is the path components corresponds to {@link PathSpec}, it would not have TypeRef component inside its component list, also it would only contain field's name
     */
    ArrayDeque<String> _schemaPathSpec = new ArrayDeque<>();
    DataSchema _parentSchema;
    /**
     * If the context is passing down from a {@link RecordDataSchema}, this attribute will be set with the enclosing
     * {@link RecordDataSchema.Field}
     */
    RecordDataSchema.Field _enclosingField;
    /**
     * If the context is passing down from a {@link UnionDataSchema}, this attribute will be set with the enclosing
     * {@link UnionDataSchema.Member}
     */
    UnionDataSchema.Member _enclosingUnionMember;
    /**
     * This attribute tells how {@link #_currentSchema} stored in the context is linked from its parentSchema
     * For example, if the {@link CurrentSchemaEntryMode} specify the {@link #_currentSchema} is an union member of parent Schema,
     * User can expect parentSchema is a {@link UnionDataSchema} and the {@link #_enclosingUnionMember} should be the
     * enclosing union member that stores the current schema.
     *
     * also see {@link CurrentSchemaEntryMode}
     */
    CurrentSchemaEntryMode _currentSchemaEntryMode;
    /**
     * SchemaAnnotationVisitors can set customized context
     * see {@link VisitorContext}
    */
    VisitorContext _visitorContext;

    VisitorContext getVisitorContext()
    {
      return _visitorContext;
    }

    /**
     * Generate a new {@link TraverserContext} for next recursion in {@link #onRecursion(TraverserContext)}
     * @return a new {@link TraverserContext} generated for next recursion
     */
    TraverserContext getNextContext()
    {
      TraverserContext nextContext = new TraverserContext();
      nextContext.setTraversePath(new ArrayDeque<>(this.getTraversePath()));
      nextContext.setParentSchema(this.getCurrentSchema());
      nextContext.setSchemaPathSpec(new ArrayDeque<>(this.getSchemaPathSpec()));
      nextContext.setVisitorContext(this.getVisitorContext());
      nextContext.setEnclosingField(this.getEnclosingField());
      nextContext.setEnclosingUnionMember(this.getEnclosingUnionMember());
      return nextContext;

    }

    void setVisitorContext(VisitorContext visitorContext)
    {
      _visitorContext = visitorContext;
    }

    ArrayDeque<String> getSchemaPathSpec()
    {
      return _schemaPathSpec;
    }

    void setSchemaPathSpec(ArrayDeque<String> schemaPathSpec)
    {
      _schemaPathSpec = schemaPathSpec;
    }

    DataSchema getCurrentSchema()
    {
      return _currentSchema;
    }

    void setCurrentSchema(DataSchema currentSchema)
    {
      _currentSchema = currentSchema;
    }

    ArrayDeque<String> getTraversePath()
    {
      return _traversePath;
    }

    void setTraversePath(ArrayDeque<String> traversePath)
    {
      this._traversePath = traversePath;
    }

    DataSchema getParentSchema()
    {
      return _parentSchema;
    }

    void setParentSchema(DataSchema parentSchema)
    {
      _parentSchema = parentSchema;
    }

    RecordDataSchema.Field getEnclosingField()
    {
      return _enclosingField;
    }

    void setEnclosingField(RecordDataSchema.Field enclosingField)
    {
      _enclosingField = enclosingField;
    }

    UnionDataSchema.Member getEnclosingUnionMember()
    {
      return _enclosingUnionMember;
    }

    void setEnclosingUnionMember(UnionDataSchema.Member enclosingUnionMember)
    {
      _enclosingUnionMember = enclosingUnionMember;
    }

    CurrentSchemaEntryMode getCurrentSchemaEntryMode()
    {
      return _currentSchemaEntryMode;
    }

    void setCurrentSchemaEntryMode(CurrentSchemaEntryMode currentSchemaEntryMode)
    {
      _currentSchemaEntryMode = currentSchemaEntryMode;
    }
  }
}
