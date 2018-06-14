package activerecord_test

import (
	. "github.com/greenplum-db/gp-common-go-libs/activerecord"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var db *FakeActiveRecord

var _ = BeforeSuite(func() {
	db = NewFakeActiveRecord()
})
var _ = AfterSuite(func() {
	db.Close()
})

var _ = Describe("ActiveRecord", func() {
	var err error

	Context("test where", func() {
		BeforeEach(func() {
			db.CleanTokens()
			db.Select("ctime").From("database_now").Where("queries_total=?", 3)
		})
		It("should not error", func() {
			Expect(err).NotTo(HaveOccurred(), db.ExecString())
		})
		It("sql is correct", func() {
			Expect(db.String()).To(Equal("SELECT ctime FROM database_now WHERE queries_total=?"))
		})
		It("args is correct", func() {
			Expect(len(db.Args)).To(Equal(1))
			Expect(db.Args[0]).To(Equal(3))
		})

	})

	Context("test selectdistinct", func() {
		BeforeEach(func() {
			db.CleanTokens()
			db.SelectDistinct("ctime").From("database_now").Where("queries_total=?", 3)
		})
		It("should not error", func() {
			Expect(err).NotTo(HaveOccurred(), db.ExecString())
		})
		It("sql should be right", func() {
			Expect(db.String()).To(Equal("SELECT DISTINCT ctime FROM database_now WHERE queries_total=?"))
		})
		It("args is correct", func() {
			Expect(len(db.Args)).To(Equal(1))
			Expect(db.Args[0]).To(Equal(3))
		})

	})

	Context("test whereand", func() {
		BeforeEach(func() {
			db.CleanTokens()
			db.Select("ctime").From("database_history").WhereAnd([]string{"queries_total=?", "ctime<?"}, 1, "2015-10-28 18:48:00")
		})
		It("should not error", func() {
			Expect(err).NotTo(HaveOccurred(), db.ExecString())
		})
		It("sql should be right", func() {
			Expect(db.String()).To(Equal("SELECT ctime FROM database_history WHERE queries_total=? AND ctime<?"))
		})
		It("args is correct", func() {
			Expect(len(db.Args)).To(Equal(2))
			Expect(db.Args[0]).To(Equal(1))
		})

	})
	Context("test where & and", func() {
		BeforeEach(func() {
			db.CleanTokens()
			filter := new(ActiveRecord)
			filter.Where("ctime<?", "2015-05-12 21:44:00").And("ctime>=?", "2015-05-12 20:00:00")

			db.Select("ctime").From("database_history").Append(filter)
		})
		It("should not error", func() {
			Expect(err).NotTo(HaveOccurred(), db.ExecString())
		})
		It("should have 2 args", func() {
			Expect(len(db.Args)).To(Equal(2))
		})
	})

})
