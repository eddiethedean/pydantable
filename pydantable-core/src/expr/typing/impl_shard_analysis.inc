impl ExprNode {
    pub fn dtype(&self) -> DTypeDesc {
        match self {
            ExprNode::ColumnRef { dtype, .. } => dtype.clone(),
            ExprNode::Literal { dtype, .. } => dtype.clone(),
            ExprNode::BinaryOp { dtype, .. } => dtype.clone(),
            ExprNode::CompareOp { dtype, .. } => dtype.clone(),
            ExprNode::Cast { dtype, .. } => dtype.clone(),
            ExprNode::IsNull { dtype, .. } => dtype.clone(),
            ExprNode::IsNotNull { dtype, .. } => dtype.clone(),
            ExprNode::Coalesce { dtype, .. }
            | ExprNode::CaseWhen { dtype, .. }
            | ExprNode::InList { dtype, .. }
            | ExprNode::Between { dtype, .. }
            | ExprNode::StringConcat { dtype, .. }
            | ExprNode::Substring { dtype, .. }
            | ExprNode::StringLength { dtype, .. }
            | ExprNode::StringReplace { dtype, .. }
            | ExprNode::StringPredicate { dtype, .. }
            | ExprNode::StructField { dtype, .. }
            | ExprNode::StructJsonEncode { dtype, .. }
            | ExprNode::StructJsonPathMatch { dtype, .. }
            | ExprNode::StructRenameFields { dtype, .. }
            | ExprNode::StructWithFields { dtype, .. }
            | ExprNode::UnaryNumeric { dtype, .. }
            | ExprNode::StringUnary { dtype, .. }
            | ExprNode::LogicalBinary { dtype, .. }
            | ExprNode::LogicalNot { dtype, .. }
            | ExprNode::TemporalPart { dtype, .. }
            | ExprNode::ListLen { dtype, .. }
            | ExprNode::ListGet { dtype, .. }
            | ExprNode::ListContains { dtype, .. }
            | ExprNode::ListMin { dtype, .. }
            | ExprNode::ListMax { dtype, .. }
            | ExprNode::ListSum { dtype, .. }
            | ExprNode::ListMean { dtype, .. }
            | ExprNode::ListJoin { dtype, .. }
            | ExprNode::ListSort { dtype, .. }
            | ExprNode::ListUnique { dtype, .. }
            | ExprNode::StringSplit { dtype, .. }
            | ExprNode::StringExtract { dtype, .. }
            | ExprNode::StringJsonPathMatch { dtype, .. }
            | ExprNode::StringJsonDecode { dtype, .. }
            | ExprNode::DatetimeToDate { dtype, .. }
            | ExprNode::Strptime { dtype, .. }
            | ExprNode::UnixTimestamp { dtype, .. }
            | ExprNode::FromUnixTime { dtype, .. }
            | ExprNode::BinaryLength { dtype, .. }
            | ExprNode::MapLen { dtype, .. }
            | ExprNode::MapGet { dtype, .. }
            | ExprNode::MapContainsKey { dtype, .. }
            | ExprNode::MapKeys { dtype, .. }
            | ExprNode::MapValues { dtype, .. }
            | ExprNode::MapEntries { dtype, .. }
            | ExprNode::MapFromEntries { dtype, .. }
            | ExprNode::Window { dtype, .. }
            | ExprNode::RowAccum { dtype, .. }
            | ExprNode::GlobalAgg { dtype, .. }
            | ExprNode::GlobalRowCount { dtype, .. } => dtype.clone(),
        }
    }

    pub fn referenced_columns(&self) -> HashSet<String> {
        match self {
            ExprNode::ColumnRef { name, .. } => HashSet::from([name.clone()]),
            ExprNode::Literal { .. } => HashSet::new(),
            ExprNode::BinaryOp { left, right, .. } => {
                let mut out = left.referenced_columns();
                out.extend(right.referenced_columns());
                out
            }
            ExprNode::CompareOp { left, right, .. } => {
                let mut out = left.referenced_columns();
                out.extend(right.referenced_columns());
                out
            }
            ExprNode::Cast { input, .. }
            | ExprNode::IsNull { input, .. }
            | ExprNode::IsNotNull { input, .. } => input.referenced_columns(),
            ExprNode::Coalesce { exprs, .. } => {
                let mut out = HashSet::new();
                for e in exprs {
                    out.extend(e.referenced_columns());
                }
                out
            }
            ExprNode::CaseWhen {
                branches, else_, ..
            } => {
                let mut out = else_.referenced_columns();
                for (c, t) in branches {
                    out.extend(c.referenced_columns());
                    out.extend(t.referenced_columns());
                }
                out
            }
            ExprNode::InList { inner, .. } => inner.referenced_columns(),
            ExprNode::Between {
                inner, low, high, ..
            } => {
                let mut out = inner.referenced_columns();
                out.extend(low.referenced_columns());
                out.extend(high.referenced_columns());
                out
            }
            ExprNode::StringConcat { parts, .. } => {
                let mut out = HashSet::new();
                for p in parts {
                    out.extend(p.referenced_columns());
                }
                out
            }
            ExprNode::Substring {
                inner,
                start,
                length,
                ..
            } => {
                let mut out = inner.referenced_columns();
                out.extend(start.referenced_columns());
                if let Some(l) = length {
                    out.extend(l.referenced_columns());
                }
                out
            }
            ExprNode::StringLength { inner, .. }
            | ExprNode::StringReplace { inner, .. }
            | ExprNode::StringPredicate { inner, .. }
            | ExprNode::UnaryNumeric { inner, .. }
            | ExprNode::StringUnary { inner, .. }
            | ExprNode::LogicalNot { inner, .. }
            | ExprNode::TemporalPart { inner, .. }
            | ExprNode::ListLen { inner, .. }
            | ExprNode::ListMin { inner, .. }
            | ExprNode::ListMax { inner, .. }
            | ExprNode::ListSum { inner, .. }
            | ExprNode::ListMean { inner, .. }
            | ExprNode::ListJoin { inner, .. }
            | ExprNode::ListSort { inner, .. }
            | ExprNode::ListUnique { inner, .. }
            | ExprNode::StringSplit { inner, .. }
            | ExprNode::StringExtract { inner, .. }
            | ExprNode::StringJsonPathMatch { inner, .. }
            | ExprNode::StringJsonDecode { inner, .. }
            | ExprNode::DatetimeToDate { inner, .. }
            | ExprNode::Strptime { inner, .. }
            | ExprNode::UnixTimestamp { inner, .. }
            | ExprNode::FromUnixTime { inner, .. }
            | ExprNode::BinaryLength { inner, .. }
            | ExprNode::MapLen { inner, .. }
            | ExprNode::MapGet { inner, .. }
            | ExprNode::MapContainsKey { inner, .. }
            | ExprNode::MapKeys { inner, .. }
            | ExprNode::MapValues { inner, .. }
            | ExprNode::MapEntries { inner, .. }
            | ExprNode::MapFromEntries { inner, .. }
            | ExprNode::RowAccum { inner, .. } => inner.referenced_columns(),
            ExprNode::ListGet { inner, index, .. } => {
                let mut out = inner.referenced_columns();
                out.extend(index.referenced_columns());
                out
            }
            ExprNode::ListContains { inner, value, .. } => {
                let mut out = inner.referenced_columns();
                out.extend(value.referenced_columns());
                out
            }
            ExprNode::LogicalBinary { left, right, .. } => {
                let mut out = left.referenced_columns();
                out.extend(right.referenced_columns());
                out
            }
            ExprNode::StructField { base, .. }
            | ExprNode::StructJsonEncode { base, .. }
            | ExprNode::StructJsonPathMatch { base, .. }
            | ExprNode::StructRenameFields { base, .. } => base.referenced_columns(),
            ExprNode::StructWithFields { base, updates, .. } => {
                let mut out = base.referenced_columns();
                for (_, e) in updates {
                    out.extend(e.referenced_columns());
                }
                out
            }
            ExprNode::GlobalAgg { inner, .. } => inner.referenced_columns(),
            ExprNode::GlobalRowCount { .. } => HashSet::new(),
            ExprNode::Window {
                operand,
                partition_by,
                order_by,
                ..
            } => {
                let mut out = HashSet::new();
                for n in partition_by {
                    out.insert(n.clone());
                }
                for (n, _, _) in order_by {
                    out.insert(n.clone());
                }
                if let Some(op) = operand {
                    out.extend(op.referenced_columns());
                }
                out
            }
        }
    }
}
